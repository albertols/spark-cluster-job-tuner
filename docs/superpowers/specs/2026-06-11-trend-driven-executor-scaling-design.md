# Trend-Driven Proportional Executor Scaling

**Date:** 2026-06-11
**Module:** `orchestration/cluster_tuning` (single planner + auto AutoTuner + refinement vitamins)
**Status:** Design approved — pending spec review

## Problem

When a recipe's execution time grows between the reference and current snapshots, the
AutoTuner does **not** raise its executor allocation; `min`/`initial`/`max` stay frozen.
Symmetrically, when a job gets faster, allocation is never reduced to save cost.

### Evidence
`_ETL_m_ODS_ERROR_LOG_D.json`: P95 job duration roughly doubled (reference→current) yet the
emitted config is identical across both dates — `minExecutors=2`, `maxExecutors=3`,
`initialExecutors=2`. Many recipes show the same pattern: large duration deltas, flat executor
allocation.

### Root cause — three incongruent metric interpretations

1. **Executor sizing ignores duration entirely.** `planDARecipes`
   (`single/ClusterMachineAndRecipeTuner.scala:1343`) derives `min`/`initial`/`max` purely from
   executor-*usage* metrics:
   ```
   minE = max(2, roundUp(avgExecutorsPerJob))
   maxE = (p95RunMaxExecutors > minE+1) ? roundUp(p95RunMaxExecutors) : minE+1
   ```
   `p95JobDurationMs` never enters sizing. A job that doubled in runtime but kept the same
   executor-usage shape gets the identical `min=2/max=3`.

2. **The censoring trap (the actual bug for `ODS_ERROR_LOG`).** When a recipe is pinned at
   `maxExecutors=3`, its `p95RunMaxExecutors` is *clipped by that very cap* — it physically
   cannot report needing more. The starvation is expressed only through **longer duration**,
   which is the one uncensored signal — and it is discarded. The planner sees "p95RunMax still
   ≈3 → keep max=3." Self-reinforcing.

3. **`Degraded → BoostResources` re-plan is a no-op for parallelism.** `PerformanceEvolver`
   re-runs the *same* planner on current metrics; since executor-usage metrics didn't move
   (they're censored), it regenerates `min=2/max=3`.

4. **The only existing scaling path fires on the wrong comparison.** `ExecutorScaleVitamin`
   (`single/refinement/RefinementVitamins.scala`) is driven by `divergencesCurrentSnapshot` — a
   z-score *across peer recipes within today's snapshot* ("is this recipe weird vs its
   neighbours"), **not** the reference→current trend. It only ever touches `max` (fixed ×1.5),
   never `min`/`initial`, and never scales **down**.

The longitudinal duration trend — already computed by `TrendDetector.computeDeltas` — is never
wired into executor sizing. That is the gap this design closes.

## Approved decisions

| Decision | Choice |
| --- | --- |
| Scaling law | **Dampened + cap-gated** proportional. Z-score outlier path **preserved** as an additive *extra boost* for genuine outliers. |
| `min`/`initial` | **Raise `min`/`initial` toward steady demand, `max` toward peak.** (Reverses the earlier "minExecutors untouched" decision, per explicit request.) |
| Down-scaling | **Conservative with hysteresis** — shrink only when Improved AND low cap-pressure AND enough runs; deadband prevents flapping. |
| Manual recipes | **Also scaled** — same proportional law applied to `spark.executor.instances`. |
| Duration driver | **Blend** `0.7·p95Ratio + 0.3·avgRatio` (each ratio guarded). |
| Bias | Cost / Balanced / Performance modulate gains (no new strategy objects). |

## Architecture

### Component 1 — `ExecutorTrendScaler` (new, pure object)

Side-effect-free; no Spark/IO. The heart of the change and the primary unit-test target.
Location: `single/refinement/ExecutorTrendScaler.scala` (sits beside the vitamin family it feeds).

**Inputs**
- reference `RecipeMetrics`, current `RecipeMetrics`
- current allocation: DA `(min, initial, max)` **or** manual `instances`
- cluster capacity (`maxExecutorsSupported`)
- `ScaleGains` (derived from `BiasMode` — Component 2)
- prior cumulative factor + `BoostState` (for lifecycle / compounding)

**Derived signals**
```
p95Ratio    = guardedRatio(current.p95JobDurationMs, reference.p95JobDurationMs)
avgRatio    = guardedRatio(current.avgJobDurationMs, reference.avgJobDurationMs)
durRatio    = 0.7*p95Ratio + 0.3*avgRatio                       // blended driver
capPressure = max(p95RunMaxExecutors / currentMax, fractionReachingCap.getOrElse(0))
confidence  = min(1, min(refRuns, curRuns) / 10.0)              // reuse TrendDetector.computeConfidence
```
`guardedRatio(c, r)` returns `1.0` when `r <= 0` or either side lacks `MinRunsForConfidence`
runs — i.e. "no usable signal → treat as no change."

**Direction**
- **UP** when `durRatio ≥ 1 + deadbandUp` **and** `capPressure ≥ capTouchRatio`:
  ```
  maxFactor = clamp(1 + gain*(durRatio-1)*confidence, 1, maxStep)
  maxE'     = clamp(roundUp(maxE * maxFactor), maxE, capacity)
  minFactor = 1 + minGain*(durRatio-1)*confidence        // minGain < gain
  minE'     = clamp(roundUp(minE * minFactor), 2, maxE'-1)
  initialE' = per policy.daInitialEqualsMin (min, or min+1 clamped)
  ```
- **DOWN** when `durRatio ≤ 1 - deadbandDown` **and** `capPressure < capTouchRatio` **and**
  `confidence ≥ downConfidenceFloor`:
  ```
  downFactor = clamp(1 - downGain*(1-durRatio)*confidence, minStep, 1)   // minStep e.g. 0.5
  // shrink toward observed demand + safety margin, never under demand or floor
  maxE'     = max(2, roundUp(p95RunMaxExecutors*(1+downSafetyMargin)), roundUp(maxE*downFactor))
  minE'     = max(2, roundUp(avgExecutorsPerJob), roundUp(minE*downFactor))   // min ≤ max-1 enforced
  ```
- **HOLD** otherwise (inside the deadband, or up-signal without cap-pressure → a non-parallel
  slowdown we deliberately do **not** throw executors at).

**Manual recipes**: a single `instances` value sized to peak. Apply `maxFactor` (UP) or the
shrink-toward-demand rule (DOWN) to `instances`; floor 2; clamp to capacity. No min/max split.

**Hysteresis**: the band between `deadbandUp` (e.g. +10%) and `deadbandDown` (e.g. −10%) yields
a no-op zone so noise between runs does not flap configs up and down.

**Output**: `TrendScaleDecision(newMin, newInitial, newMax, /* or newInstances */,
cumulativeFactor, direction: Up|Down|Hold, state: BoostState, reason: String)`.

### Component 2 — bias-derived `ScaleGains`

Maps the existing `BiasMode` (`CostBiased` / `CostPerformanceBalance` / `PerformanceBiased`,
in `single/TuningStrategies.scala`) to a gains bundle. No new strategy objects.

| Bias | `gain` | `minGain` | `maxStep` | `deadbandDown` | `downGain` |
| --- | --- | --- | --- | --- | --- |
| CostBiased | 0.35 | 0.15 | 1.5 | 0.05 (eager) | 0.6 |
| Balance (default) | 0.50 | 0.25 | 2.0 | 0.10 | 0.4 |
| PerformanceBiased | 0.70 | 0.40 | 2.5 | 0.20 (lazy) | 0.25 |

`deadbandUp`, `capTouchRatio`, `minStep`, `downSafetyMargin`, `downConfidenceFloor` are shared
defaults (CLI-overridable, Component 4). `ScaleGains.fromBias(bias, cliOverrides)` is a pure
factory — unit-tested.

### Component 3 — wiring in the AutoTuner

`auto/ClusterMachineAndRecipeAutoTuner.scala`:
- Build trend inputs from the existing `pairs` (already hold ref+current `RecipeMetrics`).
- Apply `ExecutorTrendScaler` per recipe to the cluster's emitted config
  (`-auto-scale-tuned.json` for DA, `-manually-tuned.json` for manual) **as the primary step**.
- **Compose with the preserved z-score path**: after the trend step, the existing
  `applyExecutorScaling` (z-score outlier) runs as an **additive extra boost** on `max`/instances
  for genuine peer-outliers, on top of the trend baseline. To avoid lifecycle cross-talk the two
  mechanisms stamp **separate** fields — the trend path stamps a dedicated
  `appliedTrendScaleFactor`, the z-score path keeps `appliedExecutorScaleFactor`. The combined
  effect is observable as the product of the two; each path is **clamped by its own `maxStep` and
  the cluster capacity** so neither — nor their composition — can run away.
- `BoostMetadataCarrier` extends to carry the boosted `min`/`initial` (not just `max`/memory) plus
  `appliedTrendScaleFactor` across re-plans, anchored on the recipe key (existing anchoring rule).
  DOWN is carried as a reduced cumulative factor.
- Lifecycle reuses `BoostState.{New, ReBoost, Holding}`. UP with a prior tag → `ReBoost`
  (compounded) or `Holding` (no fresh signal). DOWN reduces the cumulative factor.

### Component 4 — CLI (back-compatible)

New flags, all with defaults; **every existing flag unchanged**:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--trend-scale-gain` | (bias) | Override up `gain`. |
| `--trend-min-gain` | (bias) | Override `minGain`. |
| `--trend-scale-deadband` | 0.10 | `deadbandUp` (UP trigger threshold). |
| `--trend-scale-max-step` | (bias) | Per-run `maxStep` clamp. |
| `--trend-downscale-enabled` | true | Toggle DOWN entirely. |
| `--trend-scale-min-runs` | 5 | `MinRunsForConfidence` for a usable ratio. |

Existing `--executor-scale-factor` / `--scale-z-threshold` / `--scale-cap-touch-ratio` continue to
drive the preserved z-score *extra-boost* path.

### Component 5 — observability

- `GenerationSummary` `boost_groups`: extend the `executor_scale` entry to record direction
  (up/down/hold) and source (`trend` vs `z-score`). Per the cluster-tuning skill, a new code/source
  also needs a CSS color in `frontend/style.css` — flagged for the frontend follow-up, but the
  backend emits the structured field now.
- Every UP/DOWN/HOLD logs the driver values (`durRatio`, `capPressure`, `confidence`, factors)
  and why it fired or held — matching the existing "every fallback logs why" convention.

## Testing (ScalaTest, Spark-free — `AnyFunSuite with Matchers`)

`ExecutorTrendScalerSpec`:
1. **Censoring case** — capped (`capPressure≈1`), `durRatio≈2` → `min` and `max` both rise
   (the `ODS_ERROR_LOG` scenario).
2. **Non-cap-touch slowdown** — `durRatio≈2`, `capPressure` low → HOLD (no scale; not
   parallelism-bound).
3. **Improvement** — `durRatio≈0.6`, low cap-pressure, enough runs → conservative shrink toward
   demand, never below floor 2 / observed demand.
4. **Deadband / hysteresis** — `durRatio` in `[0.90, 1.10]` → HOLD; two-run alternating noise
   does not flap.
5. **Confidence damping** — few runs → smaller move / guarded ratio → HOLD.
6. **Bias gains** — Cost vs Balanced vs Performance produce ordered factors for identical input.
7. **Capacity clamp** — never exceeds `maxExecutorsSupported`; `min ≤ max-1` invariant holds.
8. **Compounding** — trend factor × z-score extra-boost stays within `maxStep` + capacity.
9. **Manual recipes** — `instances` scaled up/down by the same law; floor 2.
10. **Idempotence** — re-running on already-scaled config with no fresh signal → `Holding`,
    no further change.

`ScaleGainsSpec`: bias → gains mapping; CLI override precedence.

AutoTuner integration: assert emitted JSON for a synthetic censored recipe shows raised
`min`/`max`; assert an improved recipe shrinks; assert `appliedExecutorScaleFactor` round-trips
via `SimpleJsonParser` and `BoostMetadataCarrier`.

OSS-mock parity: a scenario (e.g. extend `oomHeavy` or add `durationDrift`) that produces a
censored, duration-degraded recipe so the `--full` chain exercises the new path end-to-end.

## Non-goals / YAGNI

- No change to machine selection or cost integration (b20/b21).
- No new `TuningStrategy` objects — bias reuses existing `BiasMode`.
- No frontend code in this change beyond the structured `boost_groups` field (CSS/JS follow-up).
- No removal of the z-score path — it is explicitly preserved.

## Risks & mitigations

- **Runaway compounding** → single `maxStep` + capacity clamp on the combined factor; per-run cap.
- **Flapping** → deadband/hysteresis band + confidence gating + `Holding` lifecycle.
- **Over-provisioning idle** → `min` uses the smaller `minGain`; DOWN shrinks toward demand.
- **Reversing a prior validated decision (`minExecutors` untouched)** → explicitly approved;
  recorded here and to be reflected in the memory note.
