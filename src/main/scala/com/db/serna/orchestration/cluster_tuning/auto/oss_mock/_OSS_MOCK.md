# oss_mock — synthetic input data for the Spark Cluster Job Tuner

## Why

The repository ships a single real-data fixture (`inputs/2025_12_20/`) that depends on confidential BigQuery exports. For OSS contributors, CI, and frontend dev work we need a way to produce realistic-shaped fixtures **without real data**. This package generates every CSV the tuner consumes, preserving the referential integrity the pipeline relies on.

Names are deliberately fake (`mock-cluster-001`, `mock-recipe-foo.json`) so synthetic vs real data is unambiguous when grepping logs, CSVs, and outputs.

## Package contents

| File | Role |
|---|---|
| `MockScenario.scala`  | Pure data types: `MockCluster`, `MockRecipe`, `MockIncarnation`, `MockAutoscalerProfile`, `MockExitCode`, `MockOomEvent`, `MockScenario`, `MultiDateScenario`. No I/O. |
| `MockGen.scala`       | One CSV writer per output file (`b13Csv`, `b1Csv` … `b12Csv`, `b14Csv`, `b16Csv`, `b20Csv`, `b21Csv`). Each is a pure `MockScenario => String`. `writeAll(scenario, dir)` writes all files at once. |
| `MockScenarios.scala` | Prebuilt fixtures: `minimal`, `baseline`, `oomHeavy`, `autoscaling`, `multiDateBaseline(refDate, curDate)`. |
| `OssMockMain.scala`   | CLI entry. Single-date or multi-date generation; optional `--full` chains the tuner + AutoTuner. |

## Quickstart

Single-date inputs only:

```
ClusterMachineAndRecipeAutoTuner / OssMockMain
  --date=2099_01_01 --scenario=baseline
```

Single-date inputs **and** run the tuner end-to-end:

```
OssMockMain --date=2099_01_01 --scenario=oomHeavy --full
```

Multi-date pair (so the AutoTuner produces `_auto_tuner_analysis.json` for the frontend dashboard):

```
OssMockMain
  --reference-date=2099_01_01
  --current-date=2099_01_02
  --scenario=multiDateBaseline
  --full
```

End-to-end demo of the z-score-driven features (b16 compounding/holding + executor scale-up + sortable divergences table):

```
OssMockMain
  --reference-date=2099_01_01
  --current-date=2099_01_02
  --scenario=divergenceShowcase
  --full
```

### What `--full` runs

For **single-date** scenarios:

1. Write all CSVs under `inputs/<date>/`
2. `ClusterMachineAndRecipeTuner.main(<date>)` → baseline per-cluster JSONs in `outputs/<date>/`
3. `ClusterMachineAndRecipeTunerRefinement.main(--reference-tuning-date <date>)` → applies b16 boosts in-place (this is what stamps `appliedMemoryHeapBoostFactor` so subsequent AutoTuner runs have something to carry)

For **multi-date** scenarios:

1. Write CSVs for both `<refDate>/` and `<curDate>/`
2. `SingleTuner` → `Refinement` for `<refDate>` (so the reference output JSONs already carry b16 boosts)
3. `SingleTuner` → `Refinement` for `<curDate>`
4. `ClusterMachineAndRecipeAutoTuner.main(--reference-date=<refDate> --current-date=<curDate>)` → `BoostMetadataCarrier` carry, b16 reboost lifecycle, z-score executor scale-up

Without the Refinement step in (3), the reference JSONs would have no `appliedMemoryHeapBoostFactor` tag and the AutoTuner's carry/Holding/ReBoost path would never visibly fire.

## Available scenarios

| Name | Clusters | Recipes | Diagnostics | Autoscaling | Use for |
|---|---|---|---|---|---|
| `minimal`     | 1 | 2 | none | none | Smallest path through the tuner; unit-test fodder |
| `baseline`    | 4 | mixed | 1 b14 exit | 2 of 4 clusters have schedules; cluster 4 has 2 incarnations | General dev / demo |
| `oomHeavy`    | 3 | mixed | several b14 exits + b16 OOM events | none | Driver-promotion / diagnostics UI |
| `autoscaling` | 3 | mixed | none | All clusters have rich SCALE_UP/DOWN schedules | Cost / b22 step-function UI |
| `multiDateBaseline` (multi-date) | derived from `baseline` | drift: cluster-001 ↑duration, cluster-003 ↓duration, cluster-004 dropped, mock-cluster-new added | inherits | inherits | AutoTuner trends / correlations / divergences |
| `mixedDropAndDegrade` (multi-date) | 1 (`mock-cluster-mixed`) | 3 in ref → 2 in cur: keep-stable (unchanged), must-boost (×1.40 → degraded), was-here (dropped) | none | none | AutoTuner mixed-cluster carry-over: BoostResources path that must still preserve a `dropped_entry` recipe (with `lastTunedDate` + `keptWithoutCurrentDate`) |
| `divergenceShowcase` (multi-date) | 6 (`mock-cluster-show-*`) | 14 paired + 1 NEW on current | b16 OOM in both dates for `_DQ3_OOM_RECURRING.json`; b16 in REF only for `_RDM_BOOST_HOLDING.json` | none | End-to-end demo for the z-score-driven features: compounded b16 boost (ReBoost), b16 boost holding, and divergence-driven executor scale-up; `_CTRL_NEWCOMER.json` shows the NEW pill in the divergences table |

## Output schemas

The generator writes the same set of files for every scenario; columns and row keys exactly match what the tuner loaders expect:

- **`b13_recommendations_inputs_per_recipe_per_cluster.csv`** — flattened recipe metrics, one row per `(cluster, recipe)`. Loaded by `loadFlattened`.
- **`b1.csv` … `b12.csv`** — same data refactored across files; loaded by `loadFromIndividualCSVs` when `b13` is absent. We always write both paths so both loaders work.
- **`b14_clusters_with_nonzero_exit_codes.csv`** — `(timestamp, job_id, cluster_name, driver_exit_code, msg)`. Cluster name is triple-quoted to match the BigQuery JSON-export shape; the loader strips quotes.
- **`b16_oom_job_driver_exceptions.csv`** — OOM events, currently unconsumed by `run()` but emitted for parity / future use.
- **`b20_cluster_span_time.csv`** — one row per cluster incarnation; multi-incarnation cluster names produce multiple rows. Required for `estimated_cost_eur`.
- **`b21_cluster_autoscaler_values.csv`** — one `RECOMMENDING` row per autoscaler decision. We only emit `RECOMMENDING` events with a numeric `target_primary_workers` because that's all the cost integrator reads.

### What we do NOT write

- **DAG and timer maps** (`_dag_cluster-relationship-map.csv`, `_dag_cluster_creation_time.csv`). These live at canonical project-wide paths under `src/main/resources/composer/dwh/config/`, *not* per-date, and overwriting them would clobber real mappings. Mock cluster names resolve to `UNKNOWN_DAG_ID` / `ZERO_TIMER` in the summaries — that's expected and harmless. To get realistic `dag_id` / `timer` values for mock clusters, manually add rows to those files.

## Determinism

`MockScenario.seed` is plumbed through the API so future RNG-driven jitter (e.g. timestamp microseconds) can be reproducible. Today the generator output is deterministic from the scenario data alone — same scenario produces byte-identical CSVs whether you re-run the same invocation or rebuild from source.

## Adding a new scenario

1. In `MockScenarios.scala`, define a new `def myScenario(date: String, seed: Long = 1234L): MockScenario`. Reuse the recipe presets (`recipeLight` / `recipeMedium` / `recipeHeavy`) for consistency.
2. Add it to `MockScenarios.singleDate` (or `MockScenarios.multiDate`).
3. Add a referential-integrity test in `ScenarioSpec` (the per-scenario invariants are auto-applied if you append your scenario to the list).
4. Document it in the table above.

## Adding a new output CSV

When the tuner gains a new input file (say `b25_*.csv`):

1. Add a case class field to `MockCluster` or `MockScenario` capturing the source data.
2. Add `def b25Csv(s: MockScenario): String` to `MockGen`.
3. Append it to `MockGen.writeAll`'s file list.
4. Update the schema table above.
5. Round-trip test in `MockGenSpec`.

## Future follow-ups (not in this package)

- `.claude/skills/oss-mock-data/SKILL.md` — a skill that drives this generator from natural-language scenarios ("a 5-cluster setup with two OOM-heavy jobs, two dates a week apart").
- A real-data anonymizer that reads `inputs/<date>/`, scrambles cluster/recipe names, redacts PII, and re-emits.
