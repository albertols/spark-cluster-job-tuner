<!-- Medium publication metadata
  Title:    F1 telemetry for your Spark cluster: the BigQuery Log Analytics setup that costs nothing
  Subtitle: How a tiny Spark listener + 5 GCP Log Analytics queries replace expensive BigQuery export pipelines for cluster sizing.
  Tags:     spark, gcp, dataproc, data-engineering, observability
  Canonical URL: https://github.com/albertols/spark-cluster-job-tuner/blob/main/docs/articles/2026-05-09-part-1-telemetry.md
-->

# F1 telemetry for your Spark cluster: the BigQuery Log Analytics setup that costs nothing

> How a tiny Spark listener + 5 GCP Log Analytics queries replace expensive BigQuery export pipelines for cluster sizing.

![Dashboard hero](../images/1_hero.png)

## TL;DR

- A tiny `SparkListener` (~200 lines) emits structured executor lifecycle JSON your Spark app already runs — no extra infra.
- Five GCP Log Analytics queries (`b13`, `b14`, `b16`, `b20`, `b21`) turn that telemetry plus native Dataproc events into actionable signals for cluster sizing.
- Log Analytics' per-query pricing model is dramatically cheaper than the equivalent BigQuery exports for this workload — same data, fraction of the cost.

## The cost-vs-blindness trade-off

Sizing GCP Dataproc clusters is guesswork. Most teams pick one of two losing strategies: over-provision to be safe (visible cost — idle workers burning budget) or under-provision and hope (hidden cost — failed jobs at 3am, slow scale-up, OOMs).

Both strategies are forced by the same root cause: nobody can see what their Spark jobs actually do. The data you'd need lives in `local[*]` event streams that GCP doesn't surface, in autoscaler decisions buried in obscure log channels, in driver crash stacktraces nobody reads.

What if you could measure cheaply, surface the right signals, and feed them straight into a cluster-sizing recommendation?

That's what the Spark Cluster Job Tuner does. This article is about its **input layer** — the telemetry side. The next article walks through the tuner itself.

## Two telemetry sources, one pipeline

The pipeline pulls signals from two complementary sources, both surfaced via GCP Log Analytics:

**1. GCP-native Dataproc logs.** Dataproc emits structured logs to two log resource types:

- `cloud_dataproc_job` — Spark application logs (where your driver writes everything via log4j2 + your `SparkListener`s).
- `dataproc.googleapis.com/autoscaler` — autoscaler decisions (`min/max executor` changes, scale-up trigger reasons).

These are automatic — every Dataproc cluster emits them, no app-side wiring needed. Coarse-grained but free.

**2. The `ExecutorTrackingListener`.** This is a custom `SparkListener` (~200 lines of Scala) you add to your Spark application via one config line: `--conf spark.extraListeners=com.db.serna.utils.spark.parallelism.ExecutorTrackingListener`. It hooks into Spark's listener bus and emits structured JSON for every executor add / remove / state-change event, every stage start / completion, every task batch.

Think of it as **F1 telemetry for your Spark engine**. F1 cars stream hundreds of channels (engine RPM, tyre pressure, brake temperature, downforce) at 1000 Hz so engineers can reason about exactly what happened on each lap. Your Spark app can do the same — what executors fired up when, what tasks they ran, where they died — for the cost of a single listener registration.

The two sources together feed five BigQuery Log Analytics queries that produce the CSV exports the tuner consumes:

```mermaid
flowchart LR
  classDef document  fill:#9aa2ab,stroke:#3a4046,color:#1d1f23
  classDef process   fill:#9b59b6,stroke:#5a2d6e,color:#fff
  classDef spark     fill:#ff7a18,stroke:#8a3a00,color:#fff
  classDef cloud     fill:#4ea1ff,stroke:#1a4f8a,color:#fff
  classDef inputBoundary  stroke-width:3px,stroke-dasharray:5 3

  subgraph INPUTS ["📥 Telemetry sources"]
    direction TB
    app[fa:fa-bolt Your Spark App + ExecutorTrackingListener]:::spark
    cluster[fa:fa-cloud Dataproc cluster events]:::cloud
    autoscaler[fa:fa-cloud Dataproc autoscaler]:::cloud
  end
  class app,cluster,autoscaler inputBoundary

  logs[fa:fa-cloud GCP Log Analytics]:::cloud
  csv[fa:fa-file-csv inputs/&lt;date&gt;/*.csv b13 b14 b16 b20 b21]:::document
  tuner[fa:fa-cog Tuner]:::process

  app -->|"executor lifecycle JSON"| logs
  cluster -->|"native log stream"| logs
  autoscaler -->|"native log stream"| logs
  logs -->|"BigQuery exports"| csv
  csv -->|"mvn"| tuner
```

## The five queries: what each captures

Each Log Analytics query is named `bNN_<purpose>.sql` and ships with a structured 6-line header (`Purpose / Telemetry / GCP source / App source / Consumed`) so contributors can navigate the actual SQL files. In plain language:

- **`b13_recommendations_inputs_per_recipe_per_cluster.sql`** — the workhorse. Joins `cloud_dataproc_job` events with `ExecutorTrackingListener` payloads to compute per-recipe baseline metrics: average executors used, p95 run duration, max executors observed, run counts, fraction of runs hitting the autoscaler cap. **Both** GCP-native + app-side.
- **`b14_clusters_with_nonzero_exit_codes.sql`** — clusters where job exit codes signal driver eviction (typically YARN preemption or OOM-kill). **GCP-native** only. Feeds the boost-lifecycle vitamin that bumps driver memory on the next replan.
- **`b16_oom_job_driver_exceptions.sql`** — driver-side Java heap OOM events per recipe. Pulls Spark driver stacktraces (`cloud_dataproc_job` + `log_name=dataproc.job.driver`) AND correlates with `ExecutorTrackingListener` lifecycle events for context. **Both**. Feeds the heap-boost vitamin.
- **`b20_cluster_span_time.sql`** — per-cluster wall-clock active window (from / to timestamps). **GCP-native**. Drives cluster cost computation and per-recipe attribution windows.
- **`b21_cluster_autoscaler_values.sql`** — autoscaler scale events (`min/max` executor changes over time). **GCP-native**, sourced from the dedicated `dataproc.googleapis.com/autoscaler` log stream. Drives the autoscaling-lens visualisation and step-function cost.

You export each as CSV from Log Analytics' UI, drop them in `inputs/<YYYY_MM_DD>/`, and the tuner takes it from there.

## The cost angle: Log Analytics vs BigQuery

The same data could in principle live in BigQuery via Dataproc's BigQuery export pipeline. Why use Log Analytics instead?

**Log Analytics charges per query** — you pay for the bytes scanned during query execution, with a generous monthly free tier. **BigQuery exports** add storage cost (every byte sits in a BQ table forever) plus query cost on top.

For the cluster-tuning workload — small intermittent reads, no joins across petabyte tables, no aggregations beyond `GROUP BY`-with-time-windows — Log Analytics is dramatically cheaper. **Order-of-magnitude cheaper for this workload pattern**, in our experience. And there's a second hidden win: Log Analytics CSV exports flow through the GCP Console UI without needing a service account with `bigquery.tables.export` IAM. You don't need PRD IAM roles to download the CSVs.

So the full input loop is: wire `ExecutorTrackingListener` once, paste five SQL queries into Log Analytics, click "Export to CSV", drop the files in `inputs/<date>/`, run the tuner. No BigQuery billing surprise. No PRD permission ticket.

> 💭 **[Your voice goes here]**
>
> War story / personal angle slot. Suggestions:
> - The cost-discovery moment: did you start with BigQuery exports? When did you realise Log Analytics was cheaper? What did the bill look like?
> - Why ExecutorTrackingListener exists at all — what alternative did you rule out (Spark History Server? Custom Prometheus? Something else)?
> - The "F1 telemetry" framing — how did that metaphor land? Is it yours or borrowed?

## What's next

PART_2 picks up where this article ends: with five CSV files sitting in `inputs/<date>/`, how does the tuner turn them into actionable `clusterConf` + `recipeSparkConf` blocks? It's the math, the boost lifecycle FSM, the dashboard that shows you exactly where the recommendations come from.

[Read PART_2 →](2026-05-09-part-2-tuners-and-frontend.md)

If this was useful: ⭐ the [Spark Cluster Job Tuner repo on GitHub](https://github.com/albertols/spark-cluster-job-tuner), wire `ExecutorTrackingListener` into one of your Spark apps, and let me know what you find.
