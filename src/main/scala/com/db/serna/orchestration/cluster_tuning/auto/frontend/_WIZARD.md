# New Tuning Run Wizard

Interactive wizard inside the dashboard for kicking off a fresh tuning run without
touching CLI flags from memory.

Available from the landing page as **+ New single tuning** and **+ New auto
tuning**.

## Status: Phase 1 (UI-only)

The wizard runs entirely in the browser against the existing static-file server
(`./serve.sh`, which is `python3 -m http.server` under the hood). It does **not**
launch the tuner — instead it produces validated CSVs and a ready-to-go IntelliJ
run configuration. Phase 2 will add a Scala HTTP server and a real **Start
Tuning** button.

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

## Phase 2 (deferred)

The next iteration will add:

* `auto/frontend/server/TunerService.scala` using the JDK built-in
  `com.sun.net.httpserver` (zero new runtime deps). Endpoints:
  `GET /api/health`, `GET/PUT /api/config`, `POST /api/inputs/<date>`,
  `PUT /api/inputs/<date>/csv?name=bNN.csv`, `POST /api/runs/single`,
  `POST /api/runs/auto`, `GET /api/runs/<id>/events` (long-poll).
* `pom.xml` profile `serve` adding `scala-maven-plugin` + `exec-maven-plugin`
  so `serve.sh --api` can boot the Scala server.
* Wizard's tuner-api.js becomes a real client; Step 4 grows a **Start Tuning**
  button with a live log pane.
* `Config.applyAt(...)` overload in `ClusterMachineAndRecipeTuner` to pass
  resolved paths instead of relying on cwd-relative defaults.
* log4j2 programmatic appender for per-run log capture.
