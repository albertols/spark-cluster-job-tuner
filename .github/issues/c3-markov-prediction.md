<!-- gh issue create
  --title "C3: Markov-chain prediction of cluster + job state"
  --label "roadmap"
  --label "statistics"
  --label "help-wanted"
-->

# C3 — Markov-chain prediction of cluster + job state

## Motivation

Today the AutoTuner is reactive: it pairs reference and current snapshots, classifies trends, and re-tunes. It doesn't predict future state — "given the last N days, where is this cluster headed?" or "what happens if I switch to `PerformanceBiasedStrategy` for this recipe?"

Markov chains over the observed log-analytics signals give us both — point predictions and scenario simulation — without an opaque ML model.

## Sub-tasks

- **State definition**: define the discrete state space (e.g. `{degraded, stable, improved, oom-risk, scale-cap-touch}`) from existing `bNN` signals.
- **Transition matrix estimation**: from historical snapshots, compute the empirical transition probabilities per recipe / per cluster.
- **Predictor**: given current state, project the most-likely state at +N snapshots. Surface in the dashboard as a "weather forecast" panel.
- **Scenario simulator**: given a hypothetical strategy / vitamin change, propagate through the chain to project cost + performance impact.
- **Tests**: synthetic state sequences with known transitions; assert the predictor recovers the matrix to within tolerance.

## Acceptance criteria

- Markov state matrix is reproducible from a fixed input — same snapshots in, same matrix out.
- Predictor handles new (no-history) recipes gracefully (fall back to global prior).
- Dashboard surface: a per-cluster "next snapshot prediction" + per-recipe scenario sliders.

## Open questions

- Discrete vs continuous-time Markov chains? CTMC if snapshot intervals vary widely; DTMC if they're regular.
- How many states? More = finer-grained but data-sparse. Probably 5-7 to start.
- Where do "newly observed" states fit (recipes with no prior history)? Bayesian smoothing with a global prior is the obvious answer.

## References

- ROADMAP.md — initiative C3
- _AUTO_TUNING.md — TrendDetector (the deterministic precursor; C3 is its probabilistic generalisation)
- StatisticalAnalysis — existing covariance / Pearson tooling we'll reuse
