# Spark Cluster Job Tuner

> Cost-optimise your GCP Dataproc Spark jobs from real telemetry — no agents, no vendor lock-in, just CSV in / cluster config out.

[![CI](https://github.com/albertols/spark-cluster-job-tuner/actions/workflows/ci.yml/badge.svg)](https://github.com/albertols/spark-cluster-job-tuner/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/albertols/spark-cluster-job-tuner/branch/main/graph/badge.svg)](https://codecov.io/gh/albertols/spark-cluster-job-tuner)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Scala](https://img.shields.io/badge/Scala-2.12.18-red.svg)](#)
[![Spark](https://img.shields.io/badge/Spark-3.5.3-orange.svg)](#)
[![Java](https://img.shields.io/badge/Java-11-blue.svg)](#)

![Dashboard hero](docs/images/1_hero.png)

## The 5-minute promise

Export 5 BigQuery Log Analytics queries to CSV, drop them in `inputs/<date>/`, run `mvn`, open the dashboard. That's it.

```bash
# 1. Build the slim server jar (one-time)
./mvnw -Pserve package

# 2. Run the auto-tuner on two snapshots
./mvnw -Pserve exec:java -Dexec.args="-cli auto --reference-date=2099_01_01 --current-date=2099_01_02"

# 3. Open the dashboard
./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh
# → opens http://localhost:8080/ — start here
```

Sample data ships with `2099_01_01` and `2099_01_02` so the dashboard works out of the box. Replace with real exports when you're ready.

## Why does this exist?

Sizing GCP Dataproc clusters is guesswork. You either over-provision to be safe (and burn budget on idle workers) or you under-provision and watch jobs OOM at 3am. Vendor heuristics don't know your job patterns. ML-based "optimisers" are black boxes that hide their math.

This tool reads YOUR job history — straight from GCP Log Analytics + a tiny Spark listener you wire into your apps — and recommends concrete `clusterConf` + `recipeSparkConf` blocks per recipe. No agents, no vendors, no SaaS. Just data → math → JSON config you can paste back.

## How it works

The pipeline has three stages: **telemetry → analysis → recommendation**. The telemetry comes from two complementary sources, both surfaced as BigQuery Log Analytics queries:

1. **GCP-native logs** (`resource.type='cloud_dataproc_cluster'` + `dataproc.googleapis.com/autoscaler`) — automatic, free, capture cluster lifecycle and autoscaler events.
2. **`ExecutorTrackingListener`** — a small Spark `SparkListener` you add to your application (one line in `--conf spark.extraListeners=...`). Acts like F1-style telemetry, emitting executor lifecycle + stage-progress JSON logs that GCP Log Analytics indexes for free.

Together they feed five CSV exports (`b13`, `b14`, `b16`, `b20`, `b21`) — the "Boosted Vitamins" the recipes take to get fit. (Yes, that's why `RefinementVitamins.scala` exists.)

```mermaid
flowchart LR
  app["Your Spark App<br/><i>+ ExecutorTrackingListener</i>"] -->|"executor lifecycle JSON logs"| logs["GCP Log Analytics"]
  cluster["Dataproc cluster events<br/><i>resource.type=cloud_dataproc_cluster</i>"] -->|"native log stream"| logs
  autoscaler["Dataproc autoscaler events<br/><i>dataproc.googleapis.com/autoscaler</i>"] -->|"native log stream"| logs
  logs -->|"BigQuery exports<br/>b13 b14 b16 b20 b21"| csv["inputs/&lt;date&gt;/*.csv"]
  csv -->|"mvn"| tuner["SingleTuner / AutoTuner"]
  tuner -->|"_*.json + _*.csv"| dashboard["Dashboard<br/><i>./serve.sh</i>"]
```

```mermaid
flowchart TB
  subgraph N2-32 ["N2-32 worker — 32 vCPU, 128 GB"]
    direction TB
    n1["Executor 1<br/>8 cores · 32 GB"]
    n2["Executor 2<br/>8 cores · 32 GB"]
    n3["Executor 3<br/>8 cores · 32 GB"]
    n4["Executor 4<br/>8 cores · 32 GB"]
  end
```

```mermaid
flowchart TB
  subgraph E2-32 ["E2-32 worker — 32 vCPU, 128 GB"]
    direction TB
    e1["Executor 1<br/>16 cores · 64 GB"]
    e2["Executor 2<br/>16 cores · 64 GB"]
  end
```

See [`_LOG_ANALYTICS.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/log_analytics/_LOG_ANALYTICS.md) for the full SQL schema and [`_PARALLELISM.md`](src/main/scala/com/db/serna/utils/spark/parallelism/_PARALLELISM.md) for the listener.

## Dataproc Autoscaler vs Dataproc Serverless

This tool optimises **Dataproc Autoscaler** clusters (managed GCE workers, persistent or ephemeral). It does **NOT** target **Dataproc Serverless** — the optimisation surface is fundamentally different.

| | **Dataproc Autoscaler** | **Dataproc Serverless** |
|---|---|---|
| Management | Managed VMs (you tune sizing) | NoOps (Google tunes everything) |
| Scaling unit | Worker VMs added/removed via YARN signals | CPU/memory per-job, instant |
| Startup | Pre-provisioned (fast) | ~1-2 min cold start |
| Pricing | Per-VM-hour (idle worker = paid worker) | Per-second of execution (ephemeral) |
| Min footprint | ≥1 worker always running | Zero-when-idle |
| Best for | Sustained / predictable / high-volume jobs | Sparse / unpredictable / one-off jobs |

> **Why you can't just "let it scale"** — Dataproc Autoscaler is bounded by your GCP project's real constraints. A `/24` subnet caps you at ~250 IPs across all running clusters. An `N2-32` vCPU quota of 100 caps your max executor count regardless of YARN demand. This tool surfaces both: see the dashboard's per-cluster IP-budget hint and the per-region machine quota panel.

References: [Dataproc Autoscaling docs](https://cloud.google.com/dataproc/docs/concepts/configuring-clusters/autoscaling) · [Dataproc Serverless comparison](https://cloud.google.com/dataproc-serverless/docs/concepts/dataproc-compare).

## The marquee features

### 1. Boost lifecycle — the Vitamins in action

When a recipe's driver gets evicted (`b14`) or its heap OOMs (`b16`), the tuner stamps a **boost** on the next replan. Across snapshots, the boost lifecycle keeps state: a recipe boosted last week and still showing pain this week gets **re-boosted** (compounded factor). A recipe whose pain has subsided **holds** the prior boost without re-applying. Cluster retired? Boost decays to zero.

```mermaid
stateDiagram-v2
  [*] --> New: fresh signal (b14 / b16 / z-score)
  New --> Holding: next snapshot, signal absent
  Holding --> ReBoost: signal returns
  ReBoost --> Holding: signal absent again
  Holding --> [*]: factor decays / cluster retired
  note right of Holding
    Boost factor preserved across replans
    via BoostMetadataCarrier.
  end note
```

See [`_REFINEMENT.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md) and [`_AUTO_TUNING.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md) for the lifecycle FSM and `BoostMetadataCarrier` mechanics.

### 2. Z-score executor SCALE-UP — statistical detection of cap touch

When a recipe is a duration outlier on the current snapshot (z ≥ 3.0 default) AND its `p95_run_max_executors / maxExecutors ≥ 0.5` (cap-touching), the tuner raises `spark.dynamicAllocation.maxExecutors` (×1.5 by default). No more guessing whether a job is throttled by autoscaler ceiling — the math tells you.

![Z-score scale-up](docs/images/2_z-score-cap-touch.png)

See `ExecutorScaleVitamin` in [`_REFINEMENT.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md).

### 3. Trends — Degraded / Improved / Stable / New / Dropped

The auto-tuner pairs reference and current snapshots, classifying each recipe's `p95_run_duration_ms` delta. New recipes (no prior data) and dropped ones (no current data) are surfaced separately, so you don't false-alarm on additions.

![Trends](docs/images/3_trends.png)

See `TrendDetector` and `StatisticalAnalysis` in [`_AUTO_TUNING.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md).

### 4. Cost & Autoscaling Lens

`b20` (cluster span) + `b21` (autoscaler step events) drive a per-cluster cost view: reference vs current vs projected, with the actual scale-up / scale-down events overlaid on the timeline. Sits side-by-side with the actual `clusterConf` JSON and `recipeSparkConf` blocks per recipe — every value copyable, every change diffable.

![Cost & Autoscaling Lens](docs/images/5_autoscaling.png)

See `PerformanceEvolver` in [`_AUTO_TUNING.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md).

### 5. Statistical Lens — Pearson on normalised covariances

Cluster-wise and global-wise Pearson correlations on the normalised covariances of `p95_run_max_executors / maxExecutors`. Tells you which recipes share the same scaling pattern — and lets you eyeball whether the cluster is actually tuned, mostly tuned, or fundamentally mis-sized.

![Pearson correlation](docs/images/6_pearson_correlation.png)

See `StatisticalAnalysis` in [`_AUTO_TUNING.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md).

## Quickstart

### Static-CSV mode (simplest)

1. Drop your BigQuery exports as CSV files in `src/main/resources/composer/dwh/config/cluster_tuning/inputs/<YYYY_MM_DD>/`.
2. Run the tuner: `./mvnw -Pserve exec:java -Dexec.args="<YYYY_MM_DD>"` (single tuner) or `... -cli auto --reference-date=<YYYY_MM_DD> --current-date=<YYYY_MM_DD>` (auto-tuner).
3. Open the dashboard: `./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh`.

### Dashboard-API mode (interactive)

```bash
./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh --api
```

This boots the Scala `TunerService` backend, opens the dashboard, and unlocks the **wizard** flow: pick dates, choose strategies, run the tuner, see results — all from the browser.

### Dashboard tour

The dashboard is interactive end-to-end: every value is copyable, every cluster ID deep-links to its recipes, every recipe deep-links to a side-by-side ref vs current `clusterConf` + `recipeSparkConf` view. Click around — there's no "back" button hidden in a menu, every nav uses the URL.

## Extending it

Two extension points worth knowing:

### `TuningStrategy` — pick or write your own

Strategies are concrete classes implementing `TuningStrategy`. Three ship today (`DefaultTuningStrategy`, `CostBiasedStrategy`, `PerformanceBiasedStrategy` — see `single/TuningStrategies.scala`). To add your own, implement the interface:

```scala
object MyStrategy extends TuningStrategy {
  override def name: String = "my-strategy"
  override def executorTopology: ExecutorTopologyPreset =
    ExecutorTopologyPreset(cores = 16, memoryPerCoreGb = 2)
  override def biasMode: BiasMode = BiasMode.CostPerformanceBalance
  override def quotas: Quotas = Quotas(n2 = 256, n2d = 128)
  // …additional knobs (machineSelectionPreference, etc.) in TuningStrategies.scala…
}
```

Pass it via `--strategy=my-strategy` on the CLI; the dashboard's wizard surfaces all registered strategies in `wizard.js` automatically.

See [`_DESIGN.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/single/_DESIGN.md) for the strategy protocol.

### `RefinementVitamin` — composable boost behaviour

The boost lifecycle (described above under "Boost lifecycle — the Vitamins in action") is composed of independently-applied "vitamins" defined in `RefinementVitamins.scala`. Each vitamin reads a signal (b14 driver eviction, b16 OOM, z-score scale-up, …) and emits a per-recipe boost annotation. Add a new vitamin = add a new lifecycle code (`b14`, `b16`, `executor_scale`, plus your own) + a CSS chip colour in `frontend/style.css`.

See [`_REFINEMENT.md`](src/main/scala/com/db/serna/orchestration/cluster_tuning/single/refinement/_REFINEMENT.md).

## Project status

Local-only today — the tool runs on your machine against CSV exports you produce. **GCP-deployable** is on the [roadmap](ROADMAP.md) (see `C1`).

## Roadmap

See [`ROADMAP.md`](ROADMAP.md) for the active sub-projects (OSS readiness phases) and the major future initiatives: GCP-deployable (`C1`), specialised agents (`C2`), Markov-chain prediction (`C3`).

## Contributing

PRs welcome — see [`ROADMAP.md`](ROADMAP.md) for active work and good-first-issue candidates. Full contribution guide lands in a follow-up sub-project (SP-3).

## Acknowledgements

Built on Apache Spark, Scala, and the GCP Dataproc + BigQuery + Log Analytics ecosystem. Test infra leans on ScalaTest and `holdenkarau/spark-testing-base`. Quality gates use Spotless (scalafmt) and scalafix (with SemanticDB). Coverage via Scoverage. CI via GitHub Actions.

## License

Apache License 2.0 — see [`LICENSE`](LICENSE).
