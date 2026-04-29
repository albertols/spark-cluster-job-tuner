---
name: oss-mock-data
description: Use whenever the user wants to generate, modify, debug, or extend the synthetic cluster-tuning fixtures under `src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/`. Triggers when the user mentions mock data, fake CSVs, b13/b14/b16/b20/b21 file shapes, the `OssMockMain` CLI, prebuilt scenarios (`minimal` / `baseline` / `oomHeavy` / `autoscaling` / `multiDateBaseline`), the `--full` chain that runs the tuner + AutoTuner, or feeding the auto-tuner frontend without real BigQuery exports. Apply even when the user doesn't say "oss-mock" — if they're asking "generate a 5-cluster scenario", "make a fake b21 file with two scale events", "add a new b25 to the mock generator", or "why does my mock cluster show UNKNOWN_DAG_ID", this skill applies.
---

# oss-mock-data — synthetic fixtures for the Spark Cluster Job Tuner

## Purpose

Produce realistic-shaped CSVs that the existing tuner pipeline reads, **without** depending on confidential BigQuery exports. Used for OSS contributors, CI, and frontend dev work.

The package and design are documented in detail at:
`src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/oss_mock/_OSS_MOCK.md`

Read that doc first when answering schema or design questions; this skill is a quick guide that points there.

## Files in the package

```
auto/oss_mock/
├── MockScenario.scala     # case classes, no I/O
├── MockGen.scala          # one CSV writer per output (b13Csv, b1Csv..b12Csv, b14Csv, b16Csv, b20Csv, b21Csv); writeAll(scenario, dir)
├── MockScenarios.scala    # prebuilt: minimal, baseline, oomHeavy, autoscaling, multiDateBaseline
├── OssMockMain.scala      # CLI entry
└── _OSS_MOCK.md           # package doc — read for schemas and rationale
```

Tests at `src/test/scala/.../auto/oss_mock/`: `MockGenSpec`, `ScenarioSpec`, `OssMockMainSpec`.

There is also an IntelliJ run config: `.run/oss_mock/OssMockMain.run.xml` (multi-date, baseline, `--full`).

## CLI cheatsheet

```
OssMockMain --date=YYYY_MM_DD                                       # single date, default scenario (baseline)
OssMockMain --date=YYYY_MM_DD --scenario=oomHeavy --seed=42         # custom scenario + seed
OssMockMain --reference-date=YYYY_MM_DD --current-date=YYYY_MM_DD \
            --scenario=multiDateBaseline                            # multi-date pair
OssMockMain --date=YYYY_MM_DD --full                                # also runs the single-date tuner
OssMockMain --reference-date=... --current-date=... \
            --scenario=multiDateBaseline --full                     # writes inputs, runs both single tuner passes,
                                                                    # then runs AutoTuner so frontend has _auto_tuner_analysis.json
```

`--inputs-root` defaults to `src/main/resources/composer/dwh/config/cluster_tuning/inputs` — the canonical path the tuner reads from. Don't override it when using `--full`.

## How to add a new scenario

1. In `MockScenarios.scala`, add `def myScenario(date: String, seed: Long = 1234L): MockScenario = …`. Reuse the recipe presets (`recipeLight` / `recipeMedium` / `recipeHeavy`) for consistency.
2. Register it: append to `MockScenarios.singleDate` (or `.multiDate`).
3. Don't add per-test invariants — `ScenarioSpec` already iterates over a list of scenarios; just append yours to that list and the existing invariants apply automatically.
4. Update the scenario table in `_OSS_MOCK.md`.

## How to add a new output CSV (when the tuner gains an input)

1. Add the source data to `MockCluster` (or `MockScenario` if it's cluster-agnostic).
2. Write `def bNNCsv(s: MockScenario): String` in `MockGen.scala`. **Match the loader's expected header exactly**, including column order. Cross-reference the loader by file:line.
3. Append `(filename, bNNCsv(s))` to `MockGen.writeAll`'s file list.
4. Add a round-trip test in `MockGenSpec` (parse via `Csv.parse`, optionally call the loader, assert row counts).

## Schema gotchas (most common bugs)

- **`Csv.parse` splits on bare commas — no CSV-quoting honored.** Free-text fields (b14 `msg`, b16 `latest_driver_message`) MUST NOT contain commas in the generator output. The current writers strip commas defensively. If you add a new free-text field, do the same.
- **b14 `cluster_name` is triple-quoted** (`"""mock-cluster-001"""`) to mimic the BigQuery JSON-export shape; `ClusterDiagnosticsProcessor.loadExitCodes` strips all `"` chars. Don't "clean it up" to single-quoted — you'll break parity with real exports.
- **b21 only emits `RECOMMENDING` rows with a numeric `target_primary_workers`.** The tuner's loader filters everything else out, so emitting `COOLDOWN` / `SCALING` rows is wasted bytes and confuses readers. Both `SCALE_UP` and `SCALE_DOWN` count — the cost integrator uses targets directly.
- **Every b21 event timestamp must fall inside its incarnation's b20 span.** `MockGen.writeB21` clips to `inc.spanEnd` defensively, but the scenario data should already be correct (`ScenarioSpec` asserts this).
- **Same `cluster_name` can have multiple incarnations** in one date window (b20 emits one row per `(cluster_name, incarnation_idx)`). Don't dedupe.

## Things we deliberately do NOT generate

- **DAG and timer maps** (`_dag_cluster-relationship-map.csv`, `_dag_cluster_creation_time.csv`). These live at canonical project-wide paths (`src/main/resources/composer/dwh/config/_dag_*.csv`), not per-date. Writing them would clobber real mappings. Mock cluster names resolve to `UNKNOWN_DAG_ID` / `ZERO_TIMER` in summaries — that's expected. To get realistic values, manually add rows to those files.

## Determinism

`MockScenario.seed` is plumbed through but currently unused (output is deterministic from scenario data alone). Same scenario → byte-identical CSVs across runs. If you introduce RNG-driven jitter (e.g. timestamp microseconds), wire it through the seed so determinism survives.

## When NOT to use this package

- Real-data anonymization. The generator is synthetic-from-scratch; it doesn't read or scramble real CSVs. A separate anonymizer is a potential future companion.
- One-off ad-hoc CSVs for a single test. Inline strings in the test file are fine for single-purpose fixtures (existing pattern, e.g. `ClusterMachineAndRecipeTunerSpec.scala`).

## Verification checklist (after changes)

Before claiming a generator change is done:

- [ ] `MockGenSpec`, `ScenarioSpec`, `OssMockMainSpec` all green.
- [ ] `OssMockMain --date=2099_01_01 --scenario=baseline --full` runs without errors and produces all 7 `_clusters-*.csv` + per-cluster JSONs + `_generation_summary.{json,csv}`.
- [ ] For multi-date: `--reference-date=2099_01_01 --current-date=2099_01_02 --scenario=multiDateBaseline --full` produces `_auto_tuner_analysis.json` and `_analyses_index.json`.
- [ ] `cd …/auto/frontend && ./serve.sh` — landing page lists the mocked analyses; clicking through Fleet Overview / Correlations / Divergences renders without console errors.