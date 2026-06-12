# Cluster-Utilization %, Capacity Guard, and Recipe Heatmap — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stamp every tuned recipe with `maxCoreUsagePct` / `maxMemoryUsagePct` (its max footprint as a share of the fully-autoscaled cluster), hard-clamp every recipe so it can never demand more executors than the cluster can physically schedule, and render a zoomable cores/memory heatmap in the dashboard.

**Architecture:** A pure `CapacityGuard` (per-node bin-packing math) is the single source of truth. It is used (a) inline by the single-tuner JSON emitters (`daJson`/`manualJson`) and (b) by a thin `CapacityGuardVitamin` that the AutoTuner runs as the **final** pass after the trend and z-score scalers. New `clusterConf` fields (`min_workers`, `max_workers`, `cluster_scaled_max_cores`, `cluster_scaled_max_memory_gb`) supply the denominator; existing `cluster_max_total_*` are untouched. The frontend reads the precomputed `%` straight from the per-cluster JSON.

**Tech Stack:** Scala 2.12.18, ScalaTest (`AnyFunSuite with Matchers`, Spark-free), Scallop CLI, vanilla-JS dashboard. No Maven Scala build — compile/tests run through IntelliJ (per `CLAUDE.md`). For local verification you may compile a changed file against `target/classes` + `target/lib/*.jar` with the local `scala-compiler-2.12.18.jar`, and run a single spec with `org.scalatest.tools.Runner -R <out> -s <FQCN>`.

---

## Conventions for this plan

- "Run the test" = run the named spec via the IntelliJ ScalaTest runner (or the local-jar runner above). "Expected: FAIL/PASS" describes the runner result.
- Section-banner style `// ── Title ──`; `private[cluster_tuning]` / `private[refinement]` visibility where tests need access.
- Reuse existing helpers: `Json.{obj,str,num,bool,arr,pretty}`, `SimpleJsonParser.{parseFile,parseMemoryGb}`, `RefinementPipeline.{refine,toRefinedJson}`, `ClusterMachineAndRecipeTuner.writeFile`, `AutoscalingPolicyConfig.maxWorkersForCluster`.
- Commit after every task with the shown message.
- Two phases: **Phase 1 (Tasks 1–6)** is the backend (data + guarantee) and is independently shippable; **Phase 2 (Tasks 7–10)** is fixtures, the heatmap, sample regen, and docs.

## File structure

| File | Responsibility | New/Modify |
| --- | --- | --- |
| `single/CapacityGuard.scala` | Pure math: `CapacityStatus`, `GuardResult`, `CapacityGuard.{scaledMax,guard}` | Create |
| `single/ClusterMachineAndRecipeTuner.scala` | `AutoscalingPolicyConfig.minWorkersForCluster`; `Config.maxClusterUtilRatio` + CLI parse; new `clusterConf` fields + per-recipe clamp/% in `daJson`/`manualJson` | Modify |
| `single/refinement/CapacityGuardVitamin.scala` | `CapacityGuardSignal`, `CapacityGuardBoost`, `CapacityGuardVitamin` adapter | Create |
| `auto/ClusterMachineAndRecipeAutoTuner.scala` | `--max-cluster-util-ratio` flag; `applyCapacityGuard`; call after the scale passes at both sites | Modify |
| `auto/oss_mock/MockScenarios.scala` | `capacityPressure` scenario (memory-heavy executor exceeding the ratio) | Modify |
| `auto/frontend/app.js` + `style.css` | `renderClusterUtilHeatmap` (stacked cores/memory bands, zoom/hover/click) + styles | Modify |
| `test/.../single/CapacityGuardSpec.scala` | Pure-math unit tests (incl. the bin-packing wedge case) | Create |
| `test/.../single/refinement/CapacityGuardVitaminSpec.scala` | Adapter clamps + stamps `%` on `RecipeConfig` | Create |
| `test/.../auto/oss_mock/MockScenariosCapacitySpec.scala` | Scenario shape assertion | Create |
| Docs + memory | `_CLUSTER_TUNING.md`, `_AUTO_TUNING.md`, `_REFINEMENT.md`, `CLAUDE.md`, memory note | Modify |

---

## Task 1: Pure `CapacityGuard` (per-node bin-packing)

**Files:**
- Create: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityGuard.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityGuardSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CapacityGuardSpec extends AnyFunSuite with Matchers {

  // Node n2d-standard-48 = 48 cores / 192 GB; 6 max workers; default ratio 0.90.
  private def g(
      isManual: Boolean = false,
      min: Int = 2,
      initial: Int = 2,
      max: Int = 100,
      ec: Int = 4,
      em: Int = 8,
      nodeCores: Int = 48,
      nodeMem: Int = 192,
      maxWorkers: Int = 6,
      ratio: Double = 0.90
  ): GuardResult =
    CapacityGuard.guard(isManual, min, initial, max, ec, em, nodeCores, nodeMem, maxWorkers, ratio)

  test("scaledMax multiplies per-node by maxWorkers") {
    CapacityGuard.scaledMax(48, 192, 6) shouldBe ((288, 1152))
  }

  test("plenty of headroom: no clamp, Ok, percentages computed against scaled-max") {
    val r = g(max = 10, ec = 4, em = 8) // 10 execs: 40 cores, 80 GB
    r.newMax shouldBe 10
    r.status shouldBe CapacityStatus.Ok
    r.maxCoreUsagePct shouldBe 13.9 +- 0.05 // 40/288
    r.maxMemoryUsagePct shouldBe 6.9 +- 0.05 // 80/1152
  }

  test("memory-heavy executor: per-node packing clamps BELOW the aggregate min (the wedge case)") {
    // 4c/18GB exec. Per-node = min(floor(.9*48/4)=10, floor(.9*192/18)=9) = 9 -> 9*6 = 54.
    // Aggregate min(floor(.9*288/4)=64, floor(.9*1152/18)=57) = 57. Packing (54) < aggregate (57).
    val r = g(max = 100, ec = 4, em = 18)
    r.newMax shouldBe 54
    r.status shouldBe CapacityStatus.Clamped
  }

  test("cores-binding clamp") {
    // 8c/4GB exec. Per-node = min(floor(.9*48/8)=5, floor(.9*192/4)=43) = 5 -> 30.
    val r = g(max = 100, ec = 8, em = 4)
    r.newMax shouldBe 30
    r.status shouldBe CapacityStatus.Clamped
  }

  test("manual recipe clamps instances; min==initial==max") {
    val r = g(isManual = true, min = 80, initial = 80, max = 80, ec = 8, em = 4) // cap 30
    r.newMax shouldBe 30
    r.newMin shouldBe 30
    r.newInitial shouldBe 30
    r.status shouldBe CapacityStatus.Clamped
  }

  test("min and initial are pulled down when the new max drops below them") {
    val r = g(min = 40, initial = 50, max = 100, ec = 8, em = 4) // cap 30
    r.newMax shouldBe 30
    r.newMin shouldBe 30
    r.newInitial shouldBe 30
  }

  test("tight: a single executor exceeds ratio*node but still fits the raw node") {
    // em=180 on a 192 GB node, ratio .9 -> .9*192=172.8 < 180 so perNodeRatio mem floor = 0,
    // but raw 192/180 = 1 -> Tight; cap = maxWorkers (1 per node).
    val r = g(max = 100, ec = 4, em = 180, maxWorkers = 6)
    r.newMax shouldBe 6
    r.status shouldBe CapacityStatus.Tight
  }

  test("infeasible: a single executor is bigger than a whole node") {
    val r = g(max = 100, ec = 4, em = 256) // em > nodeMem
    r.newMax shouldBe 1
    r.status shouldBe CapacityStatus.Infeasible
  }

  test("idempotence: guarding an already-guarded allocation is a no-op") {
    val once = g(max = 100, ec = 4, em = 18) // 54
    val twice = g(max = once.newMax, ec = 4, em = 18)
    twice.newMax shouldBe once.newMax
    twice.status shouldBe CapacityStatus.Ok // already at cap, nothing reduced
  }

  test("ratio override changes the cap") {
    g(max = 100, ec = 4, em = 8, ratio = 1.0).newMax shouldBe 72 // min(48*6/4, 192*6/8)=min(72,144)
    g(max = 100, ec = 4, em = 8, ratio = 0.5).newMax shouldBe 36 // floor(.5*48/4)=6 -> 36
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `CapacityGuardSpec`. Expected: FAIL to compile — `CapacityGuard` / `GuardResult` / `CapacityStatus` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `CapacityGuard.scala`:

```scala
package com.db.serna.orchestration.cluster_tuning.single

// ── Capacity status ──────────────────────────────────────────────────────────

sealed trait CapacityStatus { def label: String }
object CapacityStatus {
  case object Ok extends CapacityStatus { val label = "ok" }
  case object Clamped extends CapacityStatus { val label = "clamped" }
  case object Tight extends CapacityStatus { val label = "tight" }
  case object Infeasible extends CapacityStatus { val label = "infeasible" }
}

/**
 * Outcome of guarding one recipe. For manual recipes `newMin == newInitial == newMax` (the clamped
 * `spark.executor.instances`). Percentages are the job's max footprint as a share of the cluster's scaled-max
 * (autoscaling-policy max) cores / memory, computed from the POST-clamp executor count.
 */
final case class GuardResult(
    isManual: Boolean,
    newMin: Int,
    newInitial: Int,
    newMax: Int,
    maxCoreUsagePct: Double,
    maxMemoryUsagePct: Double,
    status: CapacityStatus
)

/**
 * Hard cap on a recipe's executor count so it can never demand more than `ratio` of what the cluster can physically
 * schedule. Capacity is computed by PER-NODE bin-packing — `maxWorkers × executorsPerNode` — NOT an aggregate
 * `totalCores/execCores`, because a memory-heavy executor fragments per node and the aggregate overestimates
 * (e.g. a 4c/18GB executor on a 48c/192GB node fits 10/node = 60 cluster-wide, but the aggregate says 64 — the extra
 * 4 would linger forever waiting for containers). The `ratio` (default 0.90) reserves headroom for `memoryOverhead`,
 * the NodeManager/OS, and the driver/AM.
 */
object CapacityGuard {

  val DefaultRatio: Double = 0.90

  private def round1(x: Double): Double = math.round(x * 10.0) / 10.0

  /** (scaledMaxCores, scaledMaxMemGb) at the cluster's autoscaling-policy max size. */
  def scaledMax(nodeCores: Int, nodeMemGb: Int, maxWorkers: Int): (Int, Int) =
    (nodeCores * maxWorkers, nodeMemGb * maxWorkers)

  def guard(
      isManual: Boolean,
      currentMin: Int,
      currentInitial: Int,
      currentMax: Int,
      execCores: Int,
      execMemGb: Int,
      nodeCores: Int,
      nodeMemGb: Int,
      maxWorkers: Int,
      ratio: Double
  ): GuardResult = {
    val (scaledCores, scaledMemGb) = scaledMax(nodeCores, nodeMemGb, maxWorkers)

    val ec = math.max(1, execCores)
    val em = math.max(1, execMemGb)
    // Physical max executors per node (no ratio) and the ratio-limited max per node.
    val perNodeRaw = math.min(nodeCores / ec, nodeMemGb / em) // integer floor for positive ints
    val perNodeRatio =
      math.min(math.floor(ratio * nodeCores / ec).toInt, math.floor(ratio * nodeMemGb / em).toInt)

    val (capExecutors, edge) =
      if (perNodeRaw <= 0) (1, CapacityStatus.Infeasible) // one executor bigger than a whole node
      else if (perNodeRatio <= 0) (math.max(1, maxWorkers), CapacityStatus.Tight) // fits raw node, not within ratio
      else (math.max(1, maxWorkers * perNodeRatio), CapacityStatus.Ok)

    val newMax = math.min(currentMax, capExecutors)
    val (nMin, nInit) =
      if (isManual) (newMax, newMax)
      else {
        val mn = math.min(currentMin, newMax)
        val ini = math.max(mn, math.min(currentInitial, newMax))
        (mn, ini)
      }

    val corePct = if (scaledCores > 0) round1(100.0 * newMax * ec / scaledCores) else 0.0
    val memPct = if (scaledMemGb > 0) round1(100.0 * newMax * em / scaledMemGb) else 0.0

    val status = edge match {
      case CapacityStatus.Ok => if (newMax < currentMax) CapacityStatus.Clamped else CapacityStatus.Ok
      case other => other
    }

    GuardResult(isManual, nMin, nInit, newMax, corePct, memPct, status)
  }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run `CapacityGuardSpec`. Expected: PASS (10 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityGuard.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityGuardSpec.scala
git commit -m "feat(tuner): pure CapacityGuard with per-node bin-packing clamp"
```

---

## Task 2: `AutoscalingPolicyConfig.minWorkersForCluster`

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala` (the `AutoscalingPolicyConfig` object, currently `:1851-1872`)
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/AutoscalingPolicyConfigSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner.AutoscalingPolicyConfig

class AutoscalingPolicyConfigSpec extends AnyFunSuite with Matchers {
  test("min workers equals the always-on primary count; max comes from the policy tier") {
    AutoscalingPolicyConfig.minWorkersForCluster(5) shouldBe 5
    AutoscalingPolicyConfig.maxWorkersForCluster(5) shouldBe 6
    AutoscalingPolicyConfig.minWorkersForCluster(3) shouldBe 3
    AutoscalingPolicyConfig.maxWorkersForCluster(3) shouldBe 4
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `AutoscalingPolicyConfigSpec`. Expected: FAIL — `minWorkersForCluster` not defined.

- [ ] **Step 3: Write minimal implementation**

In `AutoscalingPolicyConfig` (after `maxWorkersForCluster`), add:

```scala
    /** Always-on primary worker count = the autoscaling floor. (A single seam to swap in real policy min later.) */
    def minWorkersForCluster(numWorkers: Int): Int = numWorkers
```

- [ ] **Step 4: Run test to verify it passes**

Run `AutoscalingPolicyConfigSpec`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/AutoscalingPolicyConfigSpec.scala
git commit -m "feat(tuner): AutoscalingPolicyConfig.minWorkersForCluster"
```

---

## Task 3: Single-tuner CLI flag `--max-cluster-util-ratio`

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala` (`Config` `:457-464`, the `Config.apply` builder, `main` arg parsing `:1822+`)

- [ ] **Step 1: Add the field to `Config`**

Change the `Config` case class to add a defaulted field (keep it LAST so existing positional constructions in tests still compile):

```scala
  final case class Config(
      useFlattened: Boolean,
      date: String, // Provided date in `YYYY_MM_DD` format
      inputDir: File,
      outputDir: File,
      defaultMaster: MachineType,
      defaultWorker: MachineType,
      maxClusterUtilRatio: Double = CapacityGuard.DefaultRatio
  )
```

- [ ] **Step 2: Parse the flag in `main`**

Read the current `main` (`:1822+`). It already parses `--strategy=` / `--topology=` style flags from `args`. Add, alongside those:

```scala
    val maxClusterUtilRatio: Double = args
      .find(_.startsWith("--max-cluster-util-ratio="))
      .map(_.stripPrefix("--max-cluster-util-ratio=").toDouble)
      .filter(r => r > 0.0 && r <= 1.0)
      .getOrElse(CapacityGuard.DefaultRatio)
```

Then thread it into the `Config` that `main` builds (the `Config(...)` / `Config.apply(...)` construction in `main`): pass `maxClusterUtilRatio = maxClusterUtilRatio`. If `main` uses the `Config.apply(useFlattened, date)` helper, add a `.copy(maxClusterUtilRatio = maxClusterUtilRatio)` on the result before calling `run`.

- [ ] **Step 3: Verify it compiles via a no-op run**

Run `ClusterMachineAndRecipeTuner` in IntelliJ with the existing sample args (e.g. `2099_01_01`). Expected: runs unchanged; passing `--max-cluster-util-ratio=0.8` is accepted (no behavior change yet — wired in Task 4).

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala
git commit -m "feat(tuner): --max-cluster-util-ratio flag on the single tuner (default 0.90)"
```

---

## Task 4: Single-tuner emit — scaled-max clusterConf fields + per-recipe clamp + %

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala` (`manualJson` `:1384-1445`, `daJson` `:1450-…`, and their call sites `:1743-1744`)
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityEmitSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import com.db.serna.orchestration.cluster_tuning.single.ClusterMachineAndRecipeTuner._

class CapacityEmitSpec extends AnyFunSuite with Matchers {

  private val n2d48 = MachineType("n2d-standard-48", 48, 192)
  private def plan(cluster: String) =
    ClusterPlan(
      clusterName = cluster,
      workers = 5,
      masterMachineType = n2d48,
      workerMachineType = n2d48
    )

  test("daJson writes scaled-max clusterConf fields, min/max workers, and per-recipe % + clamp") {
    // Request 100 max execs of 4c/18GB -> per-node packing caps at 9*6 = 54.
    val da = Seq(RecipePlanDA("_r.json", minExecutors = 2, maxExecutors = 100, initialExecutors = 2,
      sparkExecutorCores = 4, sparkExecutorMemoryGb = 18))
    val json = daJson(plan("c1"), da, "tv", None, None, maxClusterUtilRatio = 0.90)

    json should include("\"min_workers\": 5")
    json should include("\"max_workers\": 6")
    json should include("\"cluster_scaled_max_cores\": 288")
    json should include("\"cluster_scaled_max_memory_gb\": 1152")
    json should include("\"spark.dynamicAllocation.maxExecutors\": \"54\"") // clamped
    json should include("\"maxMemoryUsagePct\":")                            // memory binds (~84%)
    json should include("\"capacityStatus\": \"clamped\"")
  }

  test("manualJson clamps instances and stamps %") {
    val m = Seq(RecipePlanManual("_m.json", sparkExecutorInstances = 100, sparkExecutorCores = 8, sparkExecutorMemoryGb = 4))
    val json = manualJson(plan("c2"), m, "tv", None, None, maxClusterUtilRatio = 0.90)
    json should include("\"spark.executor.instances\": \"30\"") // floor(.9*48/8)=5 -> 30
    json should include("\"maxCoreUsagePct\":")
  }
}
```

> Adapt the `ClusterPlan` constructor call to its real signature (read `:222`-area / `grep -n "final case class ClusterPlan"`). The intent is: a 5-worker `n2d-standard-48` cluster, a single recipe whose requested max exceeds the per-node packing cap.

- [ ] **Step 2: Run test to verify it fails**

Run `CapacityEmitSpec`. Expected: FAIL to compile — `daJson`/`manualJson` don't accept `maxClusterUtilRatio`; fields absent.

- [ ] **Step 3: Write the implementation**

In `manualJson` and `daJson`:

1. Add a parameter `maxClusterUtilRatio: Double = CapacityGuard.DefaultRatio` to **both** signatures (last param).
2. Compute the policy node range + scaled-max right after the existing `clusterMaxMemGb`/`clusterMaxCores` lines:

```scala
    val minWorkers = AutoscalingPolicyConfig.minWorkersForCluster(cluster.workers)
    val maxWorkers = AutoscalingPolicyConfig.maxWorkersForCluster(cluster.workers)
    val (scaledMaxCores, scaledMaxMemGb) =
      CapacityGuard.scaledMax(cluster.workerMachineType.cores, cluster.workerMachineType.memoryGb, maxWorkers)
```

3. Add to `baseFields` (right after `cluster_max_total_cores`):

```scala
      "min_workers" -> num(minWorkers),
      "max_workers" -> num(maxWorkers),
      "cluster_scaled_max_cores" -> num(scaledMaxCores),
      "cluster_scaled_max_memory_gb" -> num(scaledMaxMemGb),
```

4. In the per-recipe build, run the guard and use its result. For **`daJson`** replace the recipe `map` body so each plan is guarded first:

```scala
    val recipes: Seq[(String, String)] = plans.map { p =>
      val r = CapacityGuard.guard(
        isManual = false, p.minExecutors, p.initialExecutors, p.maxExecutors,
        p.sparkExecutorCores, p.sparkExecutorMemoryGb,
        cluster.workerMachineType.cores, cluster.workerMachineType.memoryGb, maxWorkers, maxClusterUtilRatio
      )
      val minTotalMemGb = r.newMin * p.sparkExecutorMemoryGb
      val maxTotalMemGb = r.newMax * p.sparkExecutorMemoryGb
      val capacityField: Seq[(String, String)] =
        if (r.status == CapacityStatus.Ok) Nil else Seq("capacityStatus" -> str(r.status.label))
      p.recipe -> obj(
        (Seq(
          "parallelizationFactor" -> num(5),
          "maxCoreUsagePct" -> num(r.maxCoreUsagePct),
          "maxMemoryUsagePct" -> num(r.maxMemoryUsagePct)
        ) ++ capacityField ++ Seq(
          "sparkOptsMap" -> obj(
            "spark.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
            "spark.closure.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
            "spark.dynamicAllocation.enabled" -> str("true"),
            "spark.dynamicAllocation.minExecutors" -> str(s"${r.newMin}"),
            "spark.dynamicAllocation.maxExecutors" -> str(s"${r.newMax}"),
            "spark.dynamicAllocation.initialExecutors" -> str(s"${r.newInitial}"),
            "spark.executor.cores" -> str(s"${p.sparkExecutorCores}"),
            "spark.executor.memory" -> str(s"${p.sparkExecutorMemoryGb}g")
          ),
          "total_executor_minimum_allocated_memory_gb" -> num(minTotalMemGb),
          "total_executor_maximum_allocated_memory_gb" -> num(maxTotalMemGb)
        )): _*
      )
    }
```

> Read the existing `daJson` recipe block first and preserve its exact `sparkOptsMap` keys/order — the snippet above mirrors the current DA shape; do not drop any key the current code emits (e.g. if the current `daJson` includes additional spark opts, keep them).

For **`manualJson`** do the analogous edit: guard with `isManual = true` and `p.sparkExecutorInstances` in all three slots, set `spark.executor.instances` to `r.newMax`, totals from `r.newMax`, and stamp the same three fields:

```scala
    val recipes: Seq[(String, String)] = plans.map { p =>
      val r = CapacityGuard.guard(
        isManual = true, p.sparkExecutorInstances, p.sparkExecutorInstances, p.sparkExecutorInstances,
        p.sparkExecutorCores, p.sparkExecutorMemoryGb,
        cluster.workerMachineType.cores, cluster.workerMachineType.memoryGb, maxWorkers, maxClusterUtilRatio
      )
      val totalMemGb = r.newMax * p.sparkExecutorMemoryGb
      val capacityField: Seq[(String, String)] =
        if (r.status == CapacityStatus.Ok) Nil else Seq("capacityStatus" -> str(r.status.label))
      p.recipe -> obj(
        (Seq(
          "parallelizationFactor" -> num(5),
          "maxCoreUsagePct" -> num(r.maxCoreUsagePct),
          "maxMemoryUsagePct" -> num(r.maxMemoryUsagePct)
        ) ++ capacityField ++ Seq(
          "sparkOptsMap" -> obj(
            "spark.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
            "spark.closure.serializer" -> str("org.apache.spark.serializer.KryoSerializer"),
            "spark.executor.instances" -> str(s"${r.newMax}"),
            "spark.executor.cores" -> str(s"${p.sparkExecutorCores}"),
            "spark.executor.memory" -> str(s"${p.sparkExecutorMemoryGb}g")
          ),
          "total_executor_minimum_allocated_memory_gb" -> num(totalMemGb),
          "total_executor_maximum_allocated_memory_gb" -> num(totalMemGb)
        )): _*
      )
    }
```

5. At the call sites (`:1743-1744`), pass the ratio:

```scala
        manualJson(clusterPlan, manualPlans, tunerVersion, driverOverride, breakdown.costTimelineJson, cfg.maxClusterUtilRatio)
      val daJsonStr: String =
        daJson(clusterPlan, daPlans, tunerVersion, driverOverride, breakdown.costTimelineJson, cfg.maxClusterUtilRatio)
```

- [ ] **Step 4: Run test to verify it passes**

Run `CapacityEmitSpec`. Expected: PASS (2 tests).

- [ ] **Step 5: Run the existing single-tuner spec for no-regression**

Run `ClusterMachineAndRecipeTunerSpec` (and any `*TunerSpec` asserting JSON). Expected: PASS. If a test asserts an exact recipe-object string, update it to include the two new `*UsagePct` fields (they are additive; fix the expected string to match).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/ClusterMachineAndRecipeTuner.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/CapacityEmitSpec.scala
git commit -m "feat(tuner): emit scaled-max clusterConf + per-recipe usage% with capacity clamp"
```

---

## Task 5: `CapacityGuardVitamin` adapter

**Files:**
- Create: `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/CapacityGuardVitamin.scala`
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/CapacityGuardVitaminSpec.scala`

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CapacityGuardVitaminSpec extends AnyFunSuite with Matchers {

  private def daRecipe(min: Int, max: Int, ec: Int, em: Int): RecipeConfig =
    RecipeConfig(
      parallelizationFactor = 5,
      sparkOptsMap = Map(
        "spark.dynamicAllocation.enabled" -> "true",
        "spark.dynamicAllocation.minExecutors" -> min.toString,
        "spark.dynamicAllocation.maxExecutors" -> max.toString,
        "spark.dynamicAllocation.initialExecutors" -> min.toString,
        "spark.executor.cores" -> ec.toString,
        "spark.executor.memory" -> s"${em}g"
      ),
      totalExecutorMinAllocatedMemoryGb = min * em,
      totalExecutorMaxAllocatedMemoryGb = max * em,
      extraFields = Map.empty
    )

  test("computeBoosts clamps a memory-heavy recipe and applyBoosts rewrites max + stamps %") {
    // node 48c/192GB, 6 workers, ratio .9; 4c/18GB exec -> cap 54.
    val sig = CapacityGuardSignal("c1", "_r.json", nodeCores = 48, nodeMemGb = 192, maxWorkers = 6, ratio = 0.90)
    val vitamin = new CapacityGuardVitamin(_ => Seq(sig))
    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 100, ec = 4, em = 18))

    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(sig), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "54"
    applied.extraFields("maxCoreUsagePct").toDouble should be > 0.0
    applied.extraFields("maxMemoryUsagePct").toDouble should be > 0.0
    applied.extraFields("capacityStatus") shouldBe "clamped"
    applied.totalExecutorMaxAllocatedMemoryGb shouldBe 54 * 18
  }

  test("within-capacity recipe is unchanged except for stamped % and no capacityStatus") {
    val sig = CapacityGuardSignal("c1", "_r.json", 48, 192, 6, 0.90)
    val vitamin = new CapacityGuardVitamin(_ => Seq(sig))
    val recipes = Map("_r.json" -> daRecipe(min = 2, max = 10, ec = 4, em = 8))
    val applied = vitamin.applyBoosts(vitamin.computeBoosts(Seq(sig), recipes), recipes)("_r.json")
    applied.sparkOptsMap("spark.dynamicAllocation.maxExecutors") shouldBe "10"
    applied.extraFields should contain key "maxCoreUsagePct"
    applied.extraFields should not contain key("capacityStatus")
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `CapacityGuardVitaminSpec`. Expected: FAIL to compile — `CapacityGuardSignal` / `CapacityGuardVitamin` not defined.

- [ ] **Step 3: Write minimal implementation**

Create `CapacityGuardVitamin.scala` (mirror `ExecutorTrendVitamin`'s structure):

```scala
package com.db.serna.orchestration.cluster_tuning.single.refinement

import java.io.File
import com.db.serna.orchestration.cluster_tuning.single.{CapacityGuard, CapacityStatus, GuardResult}

// ── Capacity guard signal & boost ────────────────────────────────────────────

/** Derived signal carrying the cluster's per-node capacity (read from clusterConf by the AutoTuner). */
final case class CapacityGuardSignal(
    clusterName: String,
    recipeFilename: String,
    nodeCores: Int,
    nodeMemGb: Int,
    maxWorkers: Int,
    ratio: Double
) extends VitaminSignal {
  val jobId: String = ""
  val description: String = s"capacity guard for $recipeFilename"
}

final case class CapacityGuardBoost(recipeFilename: String, result: GuardResult) extends VitaminBoost {
  val description: String =
    s"capacity ${result.status.label}: cores ${result.maxCoreUsagePct}% mem ${result.maxMemoryUsagePct}%"
}

// ── Capacity guard vitamin ───────────────────────────────────────────────────

/**
 * Final-pass adapter that runs [[CapacityGuard]] inside the [[RefinementPipeline]]. Stamps `maxCoreUsagePct`,
 * `maxMemoryUsagePct` (numeric) and, when non-Ok, `capacityStatus` (string) on each recipe, and clamps executor
 * settings. Has NO boost lifecycle (the guard is a physical ceiling, not a cumulative boost), so the date-aware
 * overload delegates to the 2-arg form.
 */
class CapacityGuardVitamin(
    val signalsForCluster: String => Seq[CapacityGuardSignal] = _ => Seq.empty
) extends RefinementVitamin {
  val name = "capacity_guard"
  val csvFileName = "(derived, no CSV)"
  val counterKey = "capacityClampedJobCount"
  val listKey = "capacityClampedJobList"
  val boostFieldKey = "maxCoreUsagePct"

  def loadSignals(inputDir: File, clusterName: String): Seq[VitaminSignal] = signalsForCluster(clusterName)

  def computeBoosts(signals: Seq[VitaminSignal], recipes: Map[String, RecipeConfig]): Seq[VitaminBoost] =
    signals.collect { case s: CapacityGuardSignal => s }.flatMap { sig =>
      recipes.get(sig.recipeFilename).map { rc =>
        val (isManual, min, initial, max) = extractAllocation(rc)
        val ec = intOpt(rc, "spark.executor.cores").getOrElse(8)
        val em = SimpleJsonParser.parseMemoryGb(rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g"))
        val r = CapacityGuard.guard(isManual, min, initial, max, ec, em, sig.nodeCores, sig.nodeMemGb, sig.maxWorkers, sig.ratio)
        CapacityGuardBoost(sig.recipeFilename, r)
      }
    }

  override def computeBoosts(
      signals: Seq[VitaminSignal],
      recipes: Map[String, RecipeConfig],
      currentSignals: Seq[VitaminSignal]
  ): Seq[VitaminBoost] = computeBoosts(signals, recipes)

  def applyBoosts(boosts: Seq[VitaminBoost], recipes: Map[String, RecipeConfig]): Map[String, RecipeConfig] =
    boosts.foldLeft(recipes) {
      case (cfg, CapacityGuardBoost(recipe, r)) =>
        cfg.get(recipe) match {
          case Some(rc) =>
            val em = SimpleJsonParser.parseMemoryGb(rc.sparkOptsMap.getOrElse("spark.executor.memory", "8g"))
            val updatedOpts =
              if (r.isManual) rc.sparkOptsMap.updated("spark.executor.instances", r.newMax.toString)
              else
                rc.sparkOptsMap
                  .updated("spark.dynamicAllocation.minExecutors", r.newMin.toString)
                  .updated("spark.dynamicAllocation.maxExecutors", r.newMax.toString)
                  .updated("spark.dynamicAllocation.initialExecutors", r.newInitial.toString)
            val baseExtra = rc.extraFields +
              ("maxCoreUsagePct" -> r.maxCoreUsagePct.toString) +
              ("maxMemoryUsagePct" -> r.maxMemoryUsagePct.toString)
            val updatedExtra =
              if (r.status == CapacityStatus.Ok) baseExtra - "capacityStatus"
              else baseExtra + ("capacityStatus" -> r.status.label)
            cfg.updated(
              recipe,
              rc.copy(
                sparkOptsMap = updatedOpts,
                totalExecutorMinAllocatedMemoryGb = (if (r.isManual) r.newMax else r.newMin) * em,
                totalExecutorMaxAllocatedMemoryGb = r.newMax * em,
                extraFields = updatedExtra
              )
            )
          case None => cfg
        }
      case (cfg, _) => cfg
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

  private def intOpt(rc: RecipeConfig, k: String): Option[Int] =
    rc.sparkOptsMap.get(k).flatMap(s => scala.util.Try(s.toInt).toOption)
}
```

> Note: `maxCoreUsagePct`/`maxMemoryUsagePct` are stored as numeric strings → `toRefinedJson` emits them unquoted (numbers). `capacityStatus` is a non-numeric string → emitted quoted. `SimpleJsonParser` does not round-trip these keys, which is fine — the guard recomputes them every pass. Confirm `VitaminSignal`/`VitaminBoost`/`RefinementVitamin` member names match `ExecutorTrendVitamin.scala` (`jobId`, `description`, `name`, `csvFileName`, `counterKey`, `listKey`, `boostFieldKey`, `loadSignals`, `computeBoosts`, `applyBoosts`).

- [ ] **Step 4: Run test to verify it passes**

Run `CapacityGuardVitaminSpec`. Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/CapacityGuardVitamin.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/CapacityGuardVitaminSpec.scala
git commit -m "feat(tuner): CapacityGuardVitamin adapter for the refinement pipeline"
```

---

## Task 6: AutoTuner — `--max-cluster-util-ratio` + `applyCapacityGuard` after the scale passes

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala` (Conf block near `:123`; resolve flag near `:338`; new method beside `applyTrendScaling` `:1097`; call sites after `applyExecutorScaling` at `:483` and `:681`; imports)

- [ ] **Step 1: Add the CLI flag + resolve it**

In the Scallop `Conf` block (after `scaleCapTouchRatio`):

```scala
  val maxClusterUtilRatio: ScallopOption[Double] = opt[Double](
    default = Some(0.90),
    descr = "Hard ceiling on a recipe's executors as a fraction of the cluster's per-node-packed scaled-max capacity (cores AND memory). Prevents un-schedulable over-allocation.",
    validate = r => r > 0.0 && r <= 1.0
  )
```

Where the other conf values are read into locals (near `:338`, beside `trendGains`):

```scala
    val maxClusterUtilRatio: Double = conf.maxClusterUtilRatio()
```

Add imports at the top (extend the existing `single.refinement` and add `single` imports):

```scala
import com.db.serna.orchestration.cluster_tuning.single.{CapacityGuard, CapacityStatus, GuardResult}
import com.db.serna.orchestration.cluster_tuning.single.refinement.{CapacityGuardSignal, CapacityGuardBoost, CapacityGuardVitamin}
```

- [ ] **Step 2: Add `applyCapacityGuard` beside `applyTrendScaling` (`:1097`)**

```scala
  /**
   * Final safety pass: clamp every recipe so it can never request more executors than the cluster can schedule, and
   * (re)stamp `maxCoreUsagePct` / `maxMemoryUsagePct`. Runs AFTER trend + z-score scaling. Capacity is read from the
   * clusterConf scaled-max fields written by the single tuner (`nodeCores = cluster_scaled_max_cores / max_workers`).
   *
   * Rewrites a file only when the guard clamps something OR the file is missing the `%` fields (which happens after a
   * trend/z-score rewrite dropped them). This preserves parity with the existing passes around `cost_timeline`: a
   * cluster untouched by any scaling keeps its single-tuner-emitted `%` (and `cost_timeline`); a scaled cluster — which
   * already lost `cost_timeline` to the trend/z-score rewrite — is re-stamped with fresh `%`.
   */
  private def applyCapacityGuard(clusterName: String, outputDir: File, ratio: Double): Seq[GuardResult] = {
    val fileNames = Seq(s"$clusterName-auto-scale-tuned.json", s"$clusterName-manually-tuned.json")
    val all = ArrayBuffer.empty[GuardResult]
    fileNames.foreach { fileName =>
      val file = new File(outputDir, fileName)
      if (file.exists()) {
        try {
          val config = SimpleJsonParser.parseFile(file)
          def confInt(k: String): Option[Int] =
            config.clusterConfFields.find(_._1 == k).flatMap { case (_, v) => scala.util.Try(v.toInt).toOption }
          val maxWorkers = confInt("max_workers").getOrElse(0)
          val scaledCores = confInt("cluster_scaled_max_cores").getOrElse(0)
          val scaledMem = confInt("cluster_scaled_max_memory_gb").getOrElse(0)
          if (maxWorkers <= 0 || scaledCores <= 0 || scaledMem <= 0) {
            logger.warn(s"capacity guard skipped for $clusterName/$fileName: missing scaled-max clusterConf fields")
          } else {
            val nodeCores = scaledCores / maxWorkers
            val nodeMem = scaledMem / maxWorkers
            val sigs = config.recipeOrder.map(r => CapacityGuardSignal(clusterName, r, nodeCores, nodeMem, maxWorkers, ratio))
            val lookup: String => Seq[CapacityGuardSignal] = c => if (c == clusterName) sigs else Seq.empty
            val result = RefinementPipeline.refine(config, Seq(new CapacityGuardVitamin(lookup)), Seq(outputDir))
            val decisions = result.appliedBoosts.collect { case b: CapacityGuardBoost => b.result }
            val clampedAny = decisions.exists(_.status != CapacityStatus.Ok)
            val pctMissing = {
              val have = config.rawJson.split("maxCoreUsagePct", -1).length - 1
              have < config.recipeOrder.size
            }
            if (clampedAny || pctMissing) {
              ClusterMachineAndRecipeTuner.writeFile(outputDir, fileName, RefinementPipeline.toRefinedJson(result))
            }
            all ++= decisions
            val clamped = decisions.count(_.status != CapacityStatus.Ok)
            if (clamped > 0) logger.info(s"  capacity guard on $fileName: $clamped recipe(s) clamped/tight/infeasible")
          }
        } catch {
          case e: Exception => logger.warn(s"Failed capacity guard for $clusterName/$fileName: ${e.getMessage}")
        }
      }
    }
    all.toSeq
  }
```

- [ ] **Step 3: Call it after `applyExecutorScaling` at BOTH sites**

At `:483` (KeepAsIs/Stable path) and `:681` (BoostResources/GenerateFresh path), immediately AFTER the `applyExecutorScaling(...)` block, add:

```scala
            applyCapacityGuard(clusterName, curOutputDir, maxClusterUtilRatio)
```

(The return value can be ignored for now, or accumulated into a summary in a future iteration.)

- [ ] **Step 4: Verify end-to-end on sample data**

Run `ClusterMachineAndRecipeAutoTuner` with `--reference-date=2099_01_01 --current-date=2099_01_02`. Expected: completes; logs show `capacity guard on …` only where a recipe is clamped; no exceptions. (Note: the committed `2099_01_*` outputs predate the scaled-max fields; the guard will log "skipped … missing scaled-max" for those until Task 9 regenerates them — that warning is expected here and is resolved in Task 9.)

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/ClusterMachineAndRecipeAutoTuner.scala
git commit -m "feat(tuner): AutoTuner capacity-guard final pass after trend + z-score"
```

---

## Task 7: OSS-mock `capacityPressure` scenario

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala` (add scenario; register in `multiDate`; append both perDate to `ScenarioSpec`)
- Test: `src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenariosCapacitySpec.scala`

> Follow the **oss-mock-data** skill for CSV shape/parity before editing `MockScenarios.scala`.

- [ ] **Step 1: Write the failing test**

```scala
package com.db.serna.orchestration.cluster_tuning.auto.oss_mock

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class MockScenariosCapacitySpec extends AnyFunSuite with Matchers {
  test("capacityPressure is registered and carries a memory-heavy, cap-touching recipe") {
    MockScenarios.multiDate.keySet should contain("capacityPressure")
    val md = MockScenarios.multiDate("capacityPressure")("2099_04_01", "2099_04_02", 1234L)
    val cur = md.perDate("2099_04_02")
    val recipe = cur.clusters.flatMap(_.recipes).find(_.name == "_CAPACITY_HOG.json").getOrElse(fail("missing _CAPACITY_HOG.json"))
    // High avg executor demand + cap pressure so planning wants many big executors.
    recipe.avgExecutorsPerJob should be >= 12.0
    recipe.fractionReachingCap.getOrElse(0.0) should be >= 0.5
  }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run `MockScenariosCapacitySpec`. Expected: FAIL — `capacityPressure` unknown.

- [ ] **Step 3: Implement the scenario**

In `MockScenarios.scala`, add a multi-date scenario with one small cluster (`num_workers` small so `max_workers` is small → tight capacity) and one heavy recipe whose demand drives a large executor plan that the guard must clamp. Reuse `recipeHeavy` as the base and bump demand:

```scala
  def capacityPressure(refDate: String, curDate: String, seed: Long = 1234L): MultiDateScenario = {
    val (s1, e1) = windowFor(refDate)
    val (s2, e2) = windowFor(curDate)
    def hog(name: String): MockRecipe = MockRecipe(
      name = name,
      avgExecutorsPerJob = 16.0,
      p95RunMaxExecutors = 28.0,
      avgJobDurationMs = 30 * 60000.0,
      p95JobDurationMs = 50 * 60000.0,
      runs = 20L,
      secondsAtCap = Some(1200L),
      runsReachingCap = Some(18L),
      totalRuns = Some(20L),
      fractionReachingCap = Some(0.9),
      maxConcurrentJobs = Some(2)
    )
    def cluster(start: java.time.Instant): MockCluster = MockCluster(
      name = "mock-cluster-capacity",
      recipes = Seq(hog("_CAPACITY_HOG.json")),
      incarnations = Seq(MockIncarnation(start.plus(2, ChronoUnit.HOURS), start.plus(10, ChronoUnit.HOURS)))
    )
    val ref = MockScenario("capacityPressure-reference", Seq(cluster(s1)), (s1, e1), seed)
    val cur = MockScenario("capacityPressure-current", Seq(cluster(s2)), (s2, e2), seed)
    MultiDateScenario("capacityPressure", Map(refDate -> ref, curDate -> cur))
  }
```

> Match the real `MockScenario`/`MockCluster` constructor argument order found in `MockScenario.scala` (named args as above are safest). Register it: append `"capacityPressure" -> (capacityPressure _)` to `MockScenarios.multiDate`. Append `"capacityPressure-reference" -> cap.perDate(refDate)` and `"capacityPressure-current" -> cap.perDate(curDate)` to `ScenarioSpec`'s `scenarios` list (with `private val cap = MockScenarios.capacityPressure(refDate, curDate)`).

- [ ] **Step 4: Run test to verify it passes**

Run `MockScenariosCapacitySpec` and `ScenarioSpec`. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenarios.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/MockScenariosCapacitySpec.scala \
        src/test/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/ScenarioSpec.scala
git commit -m "test(tuner): capacityPressure mock scenario exercises the capacity clamp"
```

---

## Task 8: Frontend — stacked cores/memory heatmap

**Files:**
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js`
- Modify: `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css`

> Follow the **frontend** skill: small surgical diffs, behavior preservation, a final QA pass. `app.js` is one long-line file — make targeted edits, do not reformat.

- [ ] **Step 1: Locate the insertion seam**

Find `renderClusterDetailBoosts` and the cluster-conf render that follows it (the function that builds the cluster detail body — search `renderClusterDetailBoosts` and the element that wraps the conf table, e.g. `detail-cluster-boosts` and the conf section). The heatmap is inserted **between** the boosts block and the conf table in that builder. Note how the cluster's per-recipe config object is accessed there (`recipeSparkConf`) and how recipe rows are tagged (`data-recipe`).

- [ ] **Step 2: Add the heatmap renderer**

Add this function (vanilla JS, no deps) near `renderClusterDetailBoosts`:

```javascript
// ── Cluster utilization heatmap (cores band over memory band) ───────────────
function utilTier(pct, status) {
  if (status === 'infeasible') return 'util-infeasible';
  if (status === 'clamped' || status === 'tight') return 'util-clamped';
  if (pct >= 90) return 'util-red';
  if (pct >= 80) return 'util-orange';
  if (pct >= 60) return 'util-amber';
  return 'util-green';
}

function getHeatZoom() {
  const p = new URLSearchParams(window.location.search);
  const z = parseInt(p.get('heatZoom'), 10);
  return (z >= 6 && z <= 28) ? z : 12; // cell px size
}
function setHeatZoom(px) {
  const p = new URLSearchParams(window.location.search);
  p.set('heatZoom', String(px));
  history.replaceState(null, '', '?' + p.toString());
  document.querySelectorAll('.util-heatmap').forEach(h => h.style.setProperty('--util-cell', px + 'px'));
}

function renderClusterUtilHeatmap(recipeSparkConf) {
  const names = Object.keys(recipeSparkConf || {}).sort();
  const rows = names.map(n => {
    const r = recipeSparkConf[n] || {};
    const opts = r.sparkOptsMap || {};
    const execs = opts['spark.dynamicAllocation.maxExecutors'] || opts['spark.executor.instances'] || '?';
    return {
      name: n,
      core: typeof r.maxCoreUsagePct === 'number' ? r.maxCoreUsagePct : null,
      mem: typeof r.maxMemoryUsagePct === 'number' ? r.maxMemoryUsagePct : null,
      status: r.capacityStatus || 'ok',
      execs: execs
    };
  });
  const haveData = rows.some(r => r.core !== null || r.mem !== null);
  if (!haveData) {
    return '<div class="util-heatmap-wrap"><div class="util-empty">No utilization data for this cluster (re-run the tuner to populate cores/memory %).</div></div>';
  }
  const cell = (r, kind) => {
    const pct = kind === 'core' ? r.core : r.mem;
    const tier = pct === null ? 'util-none' : utilTier(pct, r.status);
    const short = r.name.replace(/^_/, '').replace(/\.json$/, '');
    const title = `${short}\ncores ${r.core == null ? '—' : r.core + '%'} · mem ${r.mem == null ? '—' : r.mem + '%'} · ${r.execs} exec` +
      (r.status !== 'ok' ? ` · ${r.status}` : '');
    return `<div class="util-cell ${tier}" data-recipe="${r.name}" title="${title}"></div>`;
  };
  const band = (label, kind) =>
    `<div class="util-band"><div class="util-band-label">${label}</div>` +
    `<div class="util-grid">${rows.map(r => cell(r, kind)).join('')}</div></div>`;
  const z = getHeatZoom();
  return (
    '<div class="util-heatmap-wrap">' +
    '<div class="util-heatmap-head"><span class="util-title">Cluster utilization (max, single/last job)</span>' +
    '<span class="util-zoom"><button type="button" data-heatzoom="-1">−</button><button type="button" data-heatzoom="1">+</button></span></div>' +
    `<div class="util-heatmap" style="--util-cell:${z}px">` +
    band('CORES', 'core') + band('MEMORY', 'mem') +
    '</div>' +
    '<div class="util-legend"><span class="util-cell util-green"></span>&lt;60%' +
    '<span class="util-cell util-amber"></span>60–80%' +
    '<span class="util-cell util-orange"></span>80–90%' +
    '<span class="util-cell util-red"></span>≥90%' +
    '<span class="util-cell util-clamped"></span>clamped</div>' +
    '</div>'
  );
}
```

- [ ] **Step 3: Insert the heatmap into the detail builder + wire interactions**

1. In the cluster-detail builder, where the boosts HTML is concatenated before the conf table, insert the heatmap: `... + renderClusterUtilHeatmap(<the cluster's recipeSparkConf object>) + ...`. Use the same per-cluster config object the conf table already reads.

2. Add a delegated click handler (near where other detail handlers are wired, e.g. after the detail HTML is injected):

```javascript
document.querySelectorAll('[data-heatzoom]').forEach(btn => {
  btn.addEventListener('click', () => {
    const cur = getHeatZoom();
    const next = Math.max(6, Math.min(28, cur + (parseInt(btn.dataset.heatzoom, 10) * 2)));
    setHeatZoom(next);
  });
});
document.querySelectorAll('.util-cell[data-recipe]').forEach(c => {
  c.addEventListener('click', () => {
    const target = document.querySelector(`[data-recipe="${c.dataset.recipe}"].cluster-conf-row, [data-recipe="${c.dataset.recipe}"] .recipe-name-text`)
      || document.querySelector(`.cluster-conf-table [data-recipe="${c.dataset.recipe}"]`);
    if (target) { target.scrollIntoView({ behavior: 'smooth', block: 'center' }); target.classList.add('util-flash'); setTimeout(() => target.classList.remove('util-flash'), 1200); }
  });
});
```

> Adjust the click-target selector to whatever element the conf table tags with `data-recipe` (found in Step 1). If conf rows are not yet tagged with `data-recipe`, add that attribute to the conf-row render so click-through can find them.

- [ ] **Step 4: Add styles**

In `style.css`, add (match the existing dark-theme variables/palette):

```css
.util-heatmap-wrap { margin: 16px 0; }
.util-heatmap-head { display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px; }
.util-title { font-size: 12px; letter-spacing: .04em; text-transform: uppercase; opacity: .8; }
.util-zoom button { background: #2a2f3a; color: #cfd6e4; border: 1px solid #3a4150; border-radius: 4px; width: 24px; height: 22px; cursor: pointer; margin-left: 4px; }
.util-heatmap { --util-cell: 12px; display: flex; flex-direction: column; gap: 8px; }
.util-band { display: flex; align-items: flex-start; gap: 8px; }
.util-band-label { width: 56px; flex: 0 0 56px; font-size: 11px; opacity: .7; padding-top: 2px; }
.util-grid { display: grid; grid-template-columns: repeat(auto-fill, var(--util-cell)); gap: 2px; flex: 1; }
.util-cell { width: var(--util-cell); height: var(--util-cell); border-radius: 2px; cursor: pointer; }
.util-green { background: #2e7d32; }
.util-amber { background: #c9a227; }
.util-orange { background: #d97706; }
.util-red { background: #c62828; }
.util-clamped { background: #c62828; box-shadow: inset 0 0 0 2px #ff5252; }
.util-infeasible { background: repeating-linear-gradient(45deg, #c62828, #c62828 3px, #7a0000 3px, #7a0000 6px); }
.util-none { background: #3a4150; }
.util-legend { display: flex; align-items: center; gap: 6px; margin-top: 8px; font-size: 11px; opacity: .8; }
.util-legend .util-cell { width: 12px; height: 12px; cursor: default; margin-left: 10px; }
.util-empty { font-size: 12px; opacity: .6; padding: 10px; border: 1px dashed #3a4150; border-radius: 6px; }
.util-flash { outline: 2px solid #ff5252; transition: outline .2s; }
```

- [ ] **Step 5: Visual QA**

Serve the dashboard (`auto/frontend/serve.sh`) against output that has the new fields (generate it in Task 9 first, or temporarily point at a freshly tuned dir). Confirm: bands render between boosts and conf; hover shows recipe + both %s + exec count; clicking a cell scrolls to that recipe; zoom −/+ resizes and survives reload (URL param); a clamped recipe shows the inset-red cell; clusters without the fields show the empty-state note (no console errors).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/app.js \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/style.css
git commit -m "feat(frontend): cluster utilization cores/memory heatmap with zoom + click-through"
```

---

## Task 9: Regenerate sample outputs + end-to-end verification

**Files:**
- Regenerated under `src/main/resources/composer/dwh/config/cluster_tuning/outputs/` (committed sample data)

- [ ] **Step 1: Regenerate the showcase + capacity samples via `--full`**

Run `OssMockMain` for the capacity scenario and the existing showcase so the committed samples carry the new fields:

```
OssMockMain --scenario=capacityPressure --reference-date=2099_04_01 --current-date=2099_04_02 --seed=42 --full
OssMockMain --scenario=divergenceShowcase --reference-date=2099_01_01 --current-date=2099_01_02 --seed=1234 --full
```

- [ ] **Step 2: Confirm the data**

In `outputs/2099_04_02/mock-cluster-capacity-auto-scale-tuned.json`: `clusterConf` has `min_workers`/`max_workers`/`cluster_scaled_max_cores`/`cluster_scaled_max_memory_gb`; the `_CAPACITY_HOG.json` recipe has `maxCoreUsagePct`/`maxMemoryUsagePct` and (if the plan exceeded the ratio) `capacityStatus: "clamped"` with `maxExecutors` at the per-node-packed cap.

In `outputs/2099_01_02/*-auto-scale-tuned.json`: every recipe now carries the two `%` fields; no recipe shows a `%` above the ratio without a `capacityStatus`.

- [ ] **Step 3: Re-run the no-regression suites**

Run the full new spec set (`CapacityGuardSpec`, `AutoscalingPolicyConfigSpec`, `CapacityEmitSpec`, `CapacityGuardVitaminSpec`, `MockScenariosCapacitySpec`) plus the pre-existing tuner/refinement/auto/mock specs. Expected: all green. Fix any exact-JSON assertions broken by the additive fields.

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/composer/dwh/config/cluster_tuning/outputs/
git commit -m "chore(tuner): regenerate sample outputs with utilization% + capacity guard"
```

---

## Task 10: Documentation + memory

**Files:**
- Modify: `single/_CLUSTER_TUNING.md`, `auto/_AUTO_TUNING.md`, `single/refinement/_REFINEMENT.md`, `CLAUDE.md`
- Modify: `/Users/serna/.claude/projects/-Users-serna-IdeaProjects-spark-cluster-job-tuner/memory/` (new project note + `MEMORY.md` index line)

- [ ] **Step 1: Document the capacity guard + fields**

- `single/_CLUSTER_TUNING.md`: a "Cluster utilization & capacity guard" section — the new `clusterConf` fields (`min_workers`, `max_workers`, `cluster_scaled_max_cores`, `cluster_scaled_max_memory_gb`), the per-recipe `maxCoreUsagePct`/`maxMemoryUsagePct`/`capacityStatus`, the **per-node bin-packing** clamp (with the wedge example), and the `--max-cluster-util-ratio` flag (default 0.90, reserves headroom for overhead/driver).
- `single/refinement/_REFINEMENT.md`: add `CapacityGuardVitamin` to the Available Vitamins table (derived; clamps `min/initial/max`/`instances`; stamps `%`) and note it runs as the AutoTuner's final pass.
- `auto/_AUTO_TUNING.md`: add `--max-cluster-util-ratio` to the CLI arguments table and a short "Capacity guard (final pass)" subsection after the trend section.
- `CLAUDE.md`: one bullet under "Key design details" mirroring the trend/z-score bullets.

- [ ] **Step 2: Document the heatmap**

In `auto/_AUTO_TUNING.md` (frontend notes) and the frontend file-map comment: the cluster-detail heatmap (stacked cores/memory bands, color tiers, `?heatZoom=` URL state, click-through via `data-recipe`).

- [ ] **Step 3: Update memory**

Add `project_capacity_guard_and_heatmap.md` (type `project`) summarizing: the censoring/wedge rationale, per-node packing, the new fields, `--max-cluster-util-ratio`, the heatmap, and links `[[project_trend_executor_scaling]]` / `[[feedback_executor_scale_defaults]]`. Add its one-line index entry to `MEMORY.md`.

- [ ] **Step 4: Commit**

```bash
git add src/main/scala/com/db/serna/orchestration/cluster_tuning/single/_CLUSTER_TUNING.md \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md \
        src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md CLAUDE.md
git commit -m "docs(tuner): document cluster utilization %, capacity guard, and heatmap"
```
(Memory files live outside the repo and are not committed.)

---

## Final verification (run before opening a PR)

- [ ] Full new spec suite green: `CapacityGuardSpec`, `AutoscalingPolicyConfigSpec`, `CapacityEmitSpec`, `CapacityGuardVitaminSpec`, `MockScenariosCapacitySpec`.
- [ ] Pre-existing tuner/refinement/auto/mock specs green (additive fields only; fix any exact-JSON expectations).
- [ ] `OssMockMain --scenario=capacityPressure … --full` clamps `_CAPACITY_HOG.json` to its per-node-packed cap and the heatmap cell renders clamped-red.
- [ ] `OssMockMain --scenario=baseline … --full` leaves within-capacity recipes unclamped with sane `%` and no `capacityStatus`.
- [ ] Dashboard: heatmap renders between boosts and conf, hover/click/zoom work, no console errors, legacy (field-less) clusters show the empty state.
- [ ] `git log --oneline` shows one commit per task.

## Self-review notes (author)

- **Spec coverage:** clusterConf fields → Task 4; per-recipe `%` → Tasks 4/5; per-node-packing clamp → Task 1 (pure) + 4/5/6 (wiring); `--max-cluster-util-ratio` → Tasks 3 (single) + 6 (auto); heatmap → Task 8; fixture → Task 7; sample regen → Task 9; docs/memory → Task 10. All spec sections mapped.
- **Type consistency:** `CapacityGuard.guard`/`scaledMax`, `GuardResult`, `CapacityStatus.{Ok,Clamped,Tight,Infeasible}`, `CapacityGuardSignal`, `CapacityGuardBoost`, `CapacityGuardVitamin`, `applyCapacityGuard`, field names `maxCoreUsagePct`/`maxMemoryUsagePct`/`capacityStatus`/`min_workers`/`max_workers`/`cluster_scaled_max_cores`/`cluster_scaled_max_memory_gb` are used identically across tasks.
- **cost_timeline:** the AutoTuner guard rewrites only on clamp-or-missing-% (Task 6), giving exact parity with the existing trend/z-score behavior — no new `cost_timeline` loss. Documented inline.
- **Known adapt-points:** Task 4 (real `ClusterPlan`/`daJson` body), Task 7 (real `MockScenario` ctor order), Task 8 (real conf-row `data-recipe` seam) each say "read first" and give the exact target shape — not bare TODOs.
```
