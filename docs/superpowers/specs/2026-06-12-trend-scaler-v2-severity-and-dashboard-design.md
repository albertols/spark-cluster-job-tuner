# Trend Scaler v2 — Severity-Tiered Scaling, Cluster Prioritization & Dashboard Improvements

**Date:** 2026-06-12
**Module:** `orchestration/cluster_tuning` (refinement scaler + AutoTuner wiring + frontend dashboard)
**Status:** Design approved — pending spec review
**Builds on:** `2026-06-11-trend-driven-executor-scaling-design.md` (Trend Scaler v1)

## Problem

### A. The trend scaler misses large real-world degradations (CRITICAL)

Observed in production outputs (ref `2026_06_05` → cur `2026_06_12`):
`_ETL_m_DM_LKP_INSTRUMENTO_VL.json` degraded **1m 36s → 14m 57s (~9.3×)** with higher
`p95_run_max_executors`, yet the emitted config is byte-identical across dates
(`minExecutors=2`, `maxExecutors=3`, `initialExecutors=2`). Many recipes show the pattern.

Root causes in `ExecutorTrendScaler.decide` (v1), in order of likelihood for this case:

1. **Binary min-runs evidence gate.** `guardedRatio` returns `1.0` ("no signal") when
   *either* snapshot has `runs < 5`. A 9× degradation observed over 2–4 runs is silently
   erased — durRatio collapses to exactly 1.0 and the decision is an untraceable HOLD.
   Statistically this ignores effect size: extreme effects need less data.
2. **Confidence double-damping.** Runs are penalized twice: once by the gate, again by
   `conf = min(runs)/10` inside the factor.
3. **Ratio-only severity.** No absolute-magnitude term: 30s→3m (Δ2.5 min) is treated the
   same as 10m→90m (Δ80 min) at equal ratio, and `maxStep=2.0` caps even a 9× degradation
   at doubling. The user requirement is the opposite: small absolute deltas (seconds to
   ~3 min — often just resource-wait jitter) are acceptable; large absolute degradations
   (tens of minutes to hours) must scale proportionally and be prioritized.
4. **min over-raise.** When UP does fire, `minGain` raises `min` proportionally (2→5 in
   one run in the observed case after clamping). Requirement: min/initial creep slowly
   (≤ +1 per run); `max` is the elastic knob.
5. **No cross-recipe prioritization.** Each recipe decides independently; nothing ranks
   jobs by impact when several degrade in the same cluster.

### B. Dashboard gaps

1. Duration charts show only P95 — no Avg complement.
2. No visual cue on recipe entries for what the new config changed (executors/memory up/down).
3. Cluster Configuration: `min_workers`/`max_workers` not in the preferred key order;
   `capacityGuardedJobList` adds noise without insight (a long chip list).
4. No per-cluster aggregate "what changed in total" (Σ executors/cores/memory ref→cur).
5. Fleet search matches cluster names only — cannot find a recipe and jump to it.
6. Historical line charts (cost/workers/minutes/jobs over time) are unreadable all-gold
   spaghetti: every cluster present in the current run gets the same bold gold stroke.

## Design — Part A: `ExecutorTrendScaler` v2

All changes live in the pure scaler (`single/refinement/ExecutorTrendScaler.scala`), its
vitamin adapter (`ExecutorTrendVitamin` in `RefinementVitamins.scala`), and AutoTuner
wiring/CLI. Pipeline order is unchanged: **trend → z-score → capacity guard**.
Lifecycle (New/ReBoost/Holding), `appliedTrendScaleFactor` stamping, and
`BoostMetadataCarrier.carryTrendMetadata` are unchanged.

### A1. Severity model (magnitude-aware)

Per paired recipe, from blended durations (`blend = 0.7·p95 + 0.3·avg`, unchanged):

```
durRatio  = blendCur / blendRef          // relative degradation (guard: both > 0, else no signal)
deltaMin  = (blendCur − blendRef) / 60000.0   // absolute minutes lost per run
impactMin = deltaMin × curRuns            // total minutes lost per window → prioritization key
```

UP-side severity tiers — each requires **both** ratio and absolute magnitude:

| Tier       | Condition                                            | Step cap (× bias `maxStep`) |
|------------|------------------------------------------------------|------------------------------|
| Negligible | `durRatio < 1 + deadbandUp` **or** `deltaMin < 3.0`  | no scale-up                  |
| Moderate   | above deadband **and** `deltaMin ≥ 3.0`              | ×1.0                         |
| Severe     | `durRatio ≥ 2.0` **and** `deltaMin ≥ 10.0`           | ×1.5                         |
| Critical   | `durRatio ≥ 3.0` **and** `deltaMin ≥ 30.0`           | ×2.0                         |

Tier evaluation is top-down (Critical, else Severe, else Moderate, else Negligible).
With the balanced bias (`maxStep=2.0`) a Critical degradation can quadruple `max` in one
run. Ratio thresholds (2.0/3.0) and delta thresholds (10/30 min) are named constants in
`ScaleGains`'s companion; the Negligible floor (3.0 min) is CLI-overridable
(`--trend-min-delta-minutes`, default 3.0). Cap-pressure gating is unchanged
(`pressure ≥ capTouchRatio` still required for UP — the censoring-trap guard), as are the
deadbands/hysteresis.

### A2. Graduated evidence (replaces the binary 5-run gate)

`guardedRatio`'s min-runs zeroing is **removed**. Instead, with
`runs = min(refRuns, curRuns)`:

| Evidence            | Admitted tiers      | Step-cap adjustment              |
|---------------------|---------------------|----------------------------------|
| `runs ≥ 5`          | all                 | none                             |
| `2 ≤ runs ≤ 4`      | Severe, Critical    | demoted one tier (Critical→×1.5, Severe→×1.0) |
| `runs == 1`         | Critical only       | capped at ×1.5 absolute          |
| `runs == 0` (either side) | none          | no signal (hold)                 |

Once evidence is admitted, the confidence multiplier gets a floor:
`conf = clamp(runs/10, 0.5, 1.0)` — no more double-penalty.

Factor: `appliedFactor = clamp(1 + gain·(durRatio−1)·conf, 1.0, tierStepCap)`.
`newMax = max(currentMax+1, ceil(currentMax × factor))`, clamped to capacity but never
below `currentMax` (unchanged v1 behavior).

### A3. min/initial gentle creep

On UP (dynamic recipes):

```
newMin     = if (tier ≥ Severe && pressure ≥ 0.8) min(currentMin + 1, newMax − 1) else currentMin
newInitial = clamp(currentInitial, newMin, min(currentInitial + 1, newMax))
```

— `min` rises **at most +1 per run**, and only when the degradation is Severe+ and the
job is essentially pinned (pressure ≥ 0.8). `initial` follows, also moving at most +1.
On DOWN, `min` shrinks at most −1 per run: `newMin = clamp(max(steadyDemand, currentMin − 1), 2, newMax − 1)`
(demand floors unchanged). `max`/`instances` keep v1 DOWN behavior, **plus** a symmetric
absolute floor: DOWN requires `deltaMin ≤ −3.0` (saving < 3 min/run is not worth churn).
Manual recipes: single knob — `instances` scales toward `newMax` as in v1; creep rules do
not apply.

### A4. Capacity-budgeted cluster prioritization

New pure function in `ExecutorTrendScaler`:

```
prioritize(decisions: Seq[TrendScaleDecision], capacity: Option[Int], poolRatio: Double): Seq[TrendScaleDecision]
```

- Considers only UP decisions that changed (`newMax > originalMax`); others pass through.
- Ranks by `impactMin` **descending** (ties: larger durRatio first, then recipe name for determinism).
- Extra-executor pool: `P = ceil(capacity × poolRatio)` (capacity = the per-recipe
  schedulable executor ceiling already derived by the vitamin; `poolRatio` default 1.0,
  CLI `--trend-up-pool-ratio`). Each grant consumes `newMax − originalMax` from `P` in
  rank order.
- When `P` is exhausted, remaining UP candidates are **degraded, never zeroed**:
  `newMax = originalMax + 1` (min/initial/factor/cumulative recomputed consistently;
  reason annotated with `pool-exhausted`). This satisfies "prioritize highest-impact"
  without neglecting any job with a real admitted signal.
- Rationale for the pool: jobs are staggered in time, so a strict concurrent-demand
  budget would over-constrain; one cluster-capacity's worth of *increments per run* is a
  generous-but-bounded growth rate, and `CapacityGuard` remains the physical per-recipe
  clamp afterwards.
- `ExecutorTrendVitamin.computeBoosts` becomes two-phase: per-recipe `decide`, then
  cluster-wide `prioritize` over the changed decisions. No capacity ⇒ pool unlimited
  (pass-through).

### A5. Explainability

`TrendScaleDecision` gains `severity: String` ("negligible|moderate|severe|critical|n/a"),
`impactMinutes: Double`, `priorityRank: Option[Int]` (1-based among the cluster's UP
grants). The reason string includes `sev=… Δmin=… impact=… rank=…`. The AutoTuner's
`executor_trend` boost-group entries carry `severity` and `impact_minutes` per recipe
(additive JSON — frontend ignores unknown fields until B-part consumes them).

### A6. Testing (TDD)

- Unit specs (pure, no Spark): severity classification matrix (ratio × delta boundaries),
  graduated evidence admission/demotion, conf floor, min/initial +1 creep (incl. pressure
  gate), DOWN −3 min floor and −1 min creep, `prioritize` (rank order, pool exhaustion,
  no-starvation `+1` floor, determinism, pass-through when no capacity).
- Update existing `ExecutorTrendScaler` specs where v1 behavior intentionally changed
  (proportional min growth, minRuns zeroing).
- New oss-mock scenario `trendPriority`: one cluster with (a) a Critical big job
  (e.g. 10m→60m, cap-pinned), (b) a Moderate job (4m→9m, cap-pinned), (c) a high-ratio
  but Negligible job (20s→2m). End-to-end via `--full`: (a) gets the large grant first,
  (b) a grant (possibly pool-degraded), (c) holds. Assert `severity` fields in the
  generation summary.
- Replay hook: if the real `2026_06_12` b13 CSV is added to `inputs/`, add an assertion
  that `_ETL_m_DM_LKP_INSTRUMENTO_VL.json` scales up.

## Design — Part B: Dashboard

All frontend-only (`auto/frontend/app.js`, `index.html`, `style.css`); data already
client-side unless noted.

### B1. P95 ⇄ Avg duration toggle
Segmented toggle on the duration chart header; default P95. `?durMetric=avg` URL state
(omitted for p95). Tooltip always shows both P95 and Avg for ref/current
(`avg_job_duration_ms` is already in `deltas`/`current_metrics`). Max-executors chart
unchanged.

### B2. Config-change icons next to recipe names
In cluster-detail recipe cards (same post-render pattern as `annotateKeptRecipeCards`,
which already loads ref/cur cluster JSONs): diff `recipeSparkConf` per recipe and append
compact glyph chips after the name — executors up/down (`min/max` or `instances`),
memory up/down (`spark.executor.memory`). Tooltip shows the exact change ("maxExecutors
3 → 6"). CSS: up = amber, down = teal; distinct letterforms (e.g. `E▲`, `M▲`).

### B3. Cluster Configuration fixes
- `orderConfKeys`: insert `min_workers`, `max_workers` right after `num_workers`.
- Suppress the `capacityGuardedJobList` row entirely; keep `capacityGuardedJobCount`.
- New `METRIC_DOCS.capacity_guard` entry (per-node bin-packed clamp, the
  `--max-cluster-util-ratio` formula) wired as an ⓘ on the count row.

### B4. New "Cluster Trend Summary" section
New container before `#detail-cluster-conf`. From ref/cur `recipeSparkConf`:
KPI tiles — Σ minExecutors, Σ maxExecutors, Σ cores@max (max × `spark.executor.cores`),
Σ memory@max GB (`total_executor_maximum_allocated_memory_gb`), each as `ref → cur (Δ)`
with up/down arrows; plus counts and % of jobs scaled up / down / unchanged (by
max-executors/instances compare). Recipes present on only one side are counted as
new/dropped, excluded from the Δ sums, and shown as a footnote count.

### B5. Fleet-overview recipe search
`#cluster-search` also matches recipe names: a typeahead dropdown (max ~20 results,
`recipe — cluster`) below the input; click (or Enter for first) →
`navigate({cluster, recipe})` (opens cluster detail + recipe modal). Esc closes.
Existing cluster-card filtering behavior is preserved.

### B6. Historical line-chart readability
Shared helper applied to the four "over time" line charts:
- Stable per-cluster color: hash(clusterName) → HSL hue (replaces all-gold current-run strokes).
- Top-8 series by latest value: full opacity, 2px; all others 1px, ~0.25 alpha desaturated.
- Clickable legend chips for the top-8 (+ "N more"); click isolates/toggles a series;
  hover highlights a series and dims the rest.
- Existing zoom/pan and click-through-to-cluster preserved. Area/scatter/pie unchanged.

## Out of scope
- Changing the z-score pass, CapacityGuard math, or pipeline ordering.
- Backend changes for B-part beyond the additive A5 summary fields.
- Per-run variance/stddev collection (b13 schema unchanged); severity tiers act as the
  significance proxy.

## Docs
Update `_AUTO_TUNING.md` (trend section + new CLI flags), `_REFINEMENT.md` (scaler v2
semantics), `auto/oss_mock/_OSS_MOCK.md` (`trendPriority`), and the CLAUDE.md
trend-scaling bullet.

## Implementation note
Two independent tracks — Track A (Scala scaler v2, TDD, one commit per task) and Track B
(frontend, per-surface commits). A5's additive JSON lands before any B-part consumption.
