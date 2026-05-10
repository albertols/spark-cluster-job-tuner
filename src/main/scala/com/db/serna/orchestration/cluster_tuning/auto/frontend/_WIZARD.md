# New Tuning Run Wizard

Interactive wizard inside the dashboard for kicking off a fresh tuning run without
touching CLI flags from memory.

Available from the landing page as **+ New single tuning** and **+ New auto
tuning**.

## Modes

The dashboard runs in one of two modes:

### Static (Phase 1) — [`serve.sh`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh)

Backed by `python3 -m http.server`. The wizard validates CSVs in the browser
and produces:

* Per-CSV download buttons.
* A bash snippet that lands files at `<inputsPath>/<date>/`.
* A ready-to-drop IntelliJ run-config XML for `.idea/runConfigurations/`.

You then run the tuner from IntelliJ and click **Refresh dashboard**.

### API (Phase 2) — [`serve.sh --api`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh)

Backed by `TunerService` (a JDK `com.sun.net.httpserver` HTTP server in
[`server/TunerService.scala`](server/TunerService.scala)). The wizard's Step 4
gains a **Start Tuning** button that:

1. Persists `gcpProjectId` overrides to `config.local.json`.
2. Creates `<inputsPath>/<date>/` server-side.
3. Streams the staged CSVs to disk via `PUT /api/inputs/<date>/csv?name=…`.
4. POSTs `/api/runs/{single,auto}` with the parameters from Step 3.
5. Long-polls `/api/runs/<runId>/log?since=N&waitMs=8000` and streams every
   log4j2 line into a live log box.
6. On completion, navigates straight to the new analysis.

Single-run gate: the server returns 409 if a run is already in flight.

## Phase 3 — Fat JAR (`mvn -Pserve package`)

The `serve` Maven profile (in [`pom.xml`](../../../../../../../../../../pom.xml))
adds `scala-maven-plugin` + `exec-maven-plugin` + `maven-shade-plugin` and
produces a single self-contained jar:

```
mvn -Pserve package
# → target/spark-cluster-job-tuner-server.jar
```

This jar embeds Scala, log4j2, Scallop, and the compiled tuner classes. Two
ways to run it:

```
# HTTP service (same as ./serve.sh --api but standalone — no Maven needed)
java -jar target/spark-cluster-job-tuner-server.jar

# One-shot CLI — useful for cron / CI / scripted runs
java -jar target/spark-cluster-job-tuner-server.jar --cli auto \
  --reference-date=2026_04_30 --current-date=2026_05_29 \
  --strategy=cost_biased --b16-rebooting-factor=1.5
```

`./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh --api` automatically prefers the jar over `mvn exec:java` if
present, so a one-time `mvn -Pserve package` saves the Maven boot cost on
every subsequent dashboard launch.

The default IntelliJ build is **unchanged** — the `serve` profile is opt-in and
only activates the Scala compile / shade plugins when explicitly selected.

## Flow

```
  [ Mode · Dates ]  →  [ Inputs ]  →  [ Parameters ]  →  [ Run ]
```

### 1. Mode · Dates

* **Single** — pick one date (`YYYY_MM_DD`); the wizard will instruct you to land
  CSVs at `<inputsPath>/<date>/`.
* **Auto** — pick a *reference date* from the dropdown (existing date dirs found
  under `inputsPath`) and a *current date* (must be on or after the reference).

### 2. Inputs

Top of the panel: editable **GCP Project ID**, prefilled from
`config.json.gcpProjectId`. Saving produces a downloadable
`config.local.json` (git-ignored) which the dashboard's
`loadConfig()` overlays on top of `config.json` on next page load.

Five collapsible cards, one per BigQuery query:

| `bNN` | Optional? | Purpose |
| --- | --- | --- |
| `b13` | **REQUIRED** | Per-(cluster, recipe) metrics (executors, durations, cap ratio). |
| `b20` | **REQUIRED** | Cluster span time (one row per CreateCluster→DeleteCluster incarnation). Cost estimator depends on this. |
| `b14` | optional | Driver exit codes — feeds the YARN-eviction (b14) vitamin. |
| `b16` | optional | Driver Java heap OOM — feeds the b16 memory-boost vitamin. |
| `b21` | optional | Autoscaler events — refines cost via b22 step-function. |

Each card shows the SQL with `<projectId>.global._Default._Default` substituted
to the configured project, a copy button, and a drag-drop zone. Dropped CSVs are
validated by header (set-equality against the columns the Scala parser actually
reads). Row counts are stream-counted (no full file read in JS).

### 3. Parameters

Single tuner exposes `--strategy`, `--topology`, and the positional
`flattened=false` toggle.

Auto tuner exposes the full
[`AutoTunerConf`](../ClusterMachineAndRecipeAutoTuner.scala) Scallop surface:
`--strategy`, `--topology`, `--keep-historical-tuning`,
`--b16-rebooting-factor`, `--b17-rebooting-factor`,
`--divergence-z-threshold`, `--executor-scale-factor`, `--scale-z-threshold`,
`--scale-cap-touch-ratio`. Defaults and ranges mirror the Scala source. Range
errors block the **Next** button.

A read-only **Strategy comparison** table renders the values of
`DefaultTuningStrategy`, `CostBiasedStrategy`, and `PerformanceBiasedStrategy`
from [`TuningStrategies.scala`](../../single/TuningStrategies.scala) so the
choice is informed.

### 4. Run

Four panels:

1. **Save staged CSVs** — per-file download buttons + a "Download all".
2. **Land the files** — copy-pasteable bash snippet that creates
   `<inputsPath>/<date>/` and moves the downloads in.
3. **Run the tuner** — IntelliJ form values (Main class, Program arguments,
   Module, Working directory) and a one-click **Download IntelliJ run config
   (.xml)** that drops into `.idea/runConfigurations/` and is auto-detected.
   A Maven panel shows the equivalent `mvn exec:java` command for reference; it
   is gated behind a banner because `pom.xml` does not yet ship the
   `scala-maven-plugin`/`exec-maven-plugin` (Phase 2).
4. **After the tuner finishes** — a **Refresh dashboard** button that re-runs
   `discoverAnalyses()` and navigates back to the landing page so the new
   analysis appears.

## Resume

Wizard state lives in `sessionStorage`. Refresh the browser mid-flow and the
dashboard offers to resume. Staged CSV bodies are not persisted (browser
security); only their metadata is. On resume, re-drop the files.

## Project ID persistence

The wizard never writes to `config.json` directly — it produces a
`config.local.json` for the user to drop next to `config.json`. Both
`app.js` (`loadConfig()`) and the wizard read this overlay on next load.
`.gitignore` covers it.

## Backend: how Phase 2 wires through

* [`server/TunerService.scala`](server/TunerService.scala) — `main()` boots the
  HTTP server (default 127.0.0.1:8080, localhost-only). Routes:
  `GET /api/health`, `GET/PUT /api/config`,
  `GET /api/inputs`, `POST /api/inputs/<date>`, `PUT /api/inputs/<date>/csv?name=bNN.csv`,
  `POST /api/runs/{single,auto}`, `GET /api/runs/<id>`, `GET /api/runs/<id>/log?since=N`,
  `DELETE /api/runs/<id>`. Static files served from the frontend dir.
* [`server/RunRegistry.scala`](server/RunRegistry.scala) — single-run gate
  (`tryClaim`) + per-run capped log buffer with a `since=N` cursor for
  long-poll.
* [`server/RunLogAppender.scala`](server/RunLogAppender.scala) — log4j2
  programmatic `AbstractAppender` attached to the root logger for the duration
  of `run()` and detached in `finally`. No `System.out` redirect.
* [`server/JsonIO.scala`](server/JsonIO.scala) — hand-rolled JSON
  reader/writer (~250 LoC, no jackson dep).

The tuners themselves accept a `-DclusterTuning.basePath=…` system property
which the server sets to the resolved `inputsPath`/`outputsPath` from
`config.json` before invoking `run()`. The single tuner also gained a
[`Config.applyAt`](../../single/ClusterMachineAndRecipeTuner.scala) overload
so the server can pass exact `inputsBase` / `outputsBase` directories without
relying on cwd. Default `Config.apply` is unchanged — existing IntelliJ + CLI
flows keep working bit-for-bit.
