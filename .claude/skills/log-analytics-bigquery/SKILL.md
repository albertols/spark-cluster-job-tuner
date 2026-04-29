---
name: log-analytics-bigquery
description: Use whenever the user is writing, debugging, or modifying BigQuery Log Analytics SQL for the Spark Cluster Job Tuner — anything under `src/main/scala/com/db/serna/orchestration/cluster_tuning/log_analytics/` (b1.sql … b21.sql, future bNN). Triggers when the user asks about Log Analytics queries, JSON-vs-STRUCT errors, "Grouping by expressions of type JSON is not allowed", `proto_payload`, `json_payload`, `resource.labels`, audit logs, autoscaler logs, cluster lifecycle events, cluster span / incarnation logic, or BigQuery CSV export quirks. Apply even when the user doesn't say "log analytics" — if they mention `cloudaudit.googleapis.com`, `dataproc.googleapis.com/autoscaler`, `JSON_VALUE`, or paste a BigQuery error, this skill applies.
---

# log-analytics-bigquery — Log Analytics SQL gotchas for the Spark Cluster Job Tuner

## Authoritative reference

Read this in-repo doc before writing or modifying any `bNN_*.sql`:

`src/main/scala/com/db/serna/orchestration/cluster_tuning/log_analytics/_LOG_ANALYTICS.md`

It covers: source project, JSON-vs-STRUCT-vs-STRING column types, `log_name` filters per stream, lookback retention, audit-log duplicate-event collapsing, same-name-across-incarnations handling, CSV-export quirks, and the discovery-probe templates.

## Quick rules

- **`resource.labels.*` is JSON-typed** in this view → use `JSON_VALUE(resource.labels.<key>)`. `CAST(... AS STRING)` does NOT convert it for `GROUP BY`.
- **`proto_payload` is STRUCT** → direct field access (`proto_payload.audit_log.method_name`). `JSON_VALUE` on it errors.
- **`json_payload` is JSON** → `JSON_VALUE(json_payload, '$.path.to.field')`.
- **Source project**: `db-prd-rn63-pwcclake-es.global._Default._Default` (b1–b13/b15/b16/b20/b21 use this; only b14 is on the dev project for legacy reasons).
- **Audit logs duplicate** every long-running operation (request + completion entries 2–3 min apart). Collapse by run-anchor (`GROUP BY (cluster_name, prev_opposite_event_ts)` + `MIN(ts)`), not by time-window heuristics.
- **`cluster_name` is not unique** — the same name recreated multiple times in the lookback window must produce multiple rows (carry an `incarnation_idx`).
- **24h lookback** means long-lived clusters won't have explicit Create/Delete events in the window. Expose `has_explicit_create` / `has_explicit_delete` flags so consumers know.

## Workflow when adding a new bNN query

1. **Probe first.** Use the discovery template in `_LOG_ANALYTICS.md` ("Discovery probe template") to see which `log_name` carries the events you care about and what the payload shape is. Never guess paths from naming conventions.
2. **Match column types** to the rules above.
3. **Header comment** at the top of the SQL: purpose, output columns (with types), source filters, validation gates, known caveats. Mirror the b20/b21 header style.
4. **Validation gate**: share a draft with the user, have them run it in BigQuery, paste a sample of rows back, refine. The b20/b21 work shows this iterative pattern.
5. **CSV consumer**: write/update the loader in `ClusterMachineAndRecipeTuner.scala` (or sibling). Use `Csv.parse` and the existing `parseInstant` helper for timestamps.
6. **Generator parity**: if the new file becomes a real input, add a `bNNCsv` writer to the `oss_mock` package so OSS / CI flows still work — see `oss-mock-data` SKILL for that pattern.

## Common error → likely fix

| Error | Likely cause | Fix |
|---|---|---|
| `Grouping by expressions of type JSON is not allowed` | grouping on a JSON-typed column directly | wrap with `JSON_VALUE(...)` |
| `No matching signature for function JSON_VALUE: STRUCT<...>` | `JSON_VALUE` on a STRUCT (e.g. `proto_payload`) | direct STRUCT field access |
| `Cannot access field X on a value with type Y` | schema mismatch (snake_case vs camelCase) | `COALESCE(JSON_VALUE(p, '$.method_name'), JSON_VALUE(p, '$.methodName'))` |
| Empty result set when you expected rows | `log_name LIKE` too strict, or wrong project | re-run the discovery probe; verify the project matches the rest of the family |
| All `has_explicit_create=false` for known clusters | regex doesn't match real log shape | inspect a real audit row via `TO_JSON_STRING(proto_payload)`; switch from text-pattern matching to structured `proto_payload.audit_log.method_name LIKE '%.CreateCluster'` |

## When NOT to use this skill

- Writing application Scala that *consumes* a BigQuery CSV (use the `cluster-tuning` skill for loader / cost-calc / model-side work).
- Generating mock/synthetic CSV data for tests (use the `oss-mock-data` skill).
- Frontend rendering of Log-Analytics-derived data (use the `frontend` skill).