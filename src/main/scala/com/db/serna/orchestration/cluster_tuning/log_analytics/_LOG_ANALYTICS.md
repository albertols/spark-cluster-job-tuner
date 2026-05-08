# Log Analytics SQL — gotchas and conventions

Reference for writing or modifying any `bNN_*.sql` query in this directory. Read this before adding a new query — the patterns here were learned the hard way during b14, b20, and b21 development.

## Source project and table

All queries target the same Log Analytics view:

```
`db-prd-rn63-pwcclake-es.global._Default._Default`
```

(Exception: `b14_clusters_with_nonzero_exit_codes.sql` was originally written against `db-dev-apyd-pwcclake-es`. New queries should use the prd project to stay consistent with b1–b13/b15/b16/b20/b21 unless there's a specific reason otherwise.)

## Column types — JSON vs STRUCT vs STRING

The Log Analytics `_Default` view exposes three different column shapes that look superficially similar but require different access patterns. Mixing them up gives you "Grouping by expressions of type JSON is not allowed" or "No matching signature for function JSON_VALUE" errors.

| Column | Type in this view | How to extract a scalar |
|---|---|---|
| `resource.labels.cluster_name` | **JSON** | `JSON_VALUE(resource.labels.cluster_name)` |
| `resource.labels.*` (any other) | **JSON** | `JSON_VALUE(resource.labels.<key>)` |
| `proto_payload` | **STRUCT** (e.g. `STRUCT<type STRING, audit_log STRUCT<service_name STRING, method_name STRING, …>>`) | direct field access: `proto_payload.audit_log.method_name` |
| `json_payload` | **JSON** | `JSON_VALUE(json_payload, '$.path.to.field')` |
| `text_payload` | **STRING** | use as-is |
| `timestamp` | **TIMESTAMP** | use as-is |

### Common mistakes

- **`CAST(resource.labels.cluster_name AS STRING)`** — looks like it should work, doesn't. Cast preserves the JSON type for downstream `GROUP BY` purposes; you get "Grouping by expressions of type JSON is not allowed". Use `JSON_VALUE` instead.
- **`JSON_VALUE(proto_payload, '$.method_name')`** — fails with "No matching signature for function JSON_VALUE: JSON_VALUE(STRUCT<…>, STRING)". `proto_payload` is STRUCT not JSON; use direct field access.
- **`json_payload.foo.bar`** — STRUCT-style access on a JSON column fails. Use `JSON_VALUE(json_payload, '$.foo.bar')`.

## Filtering by log stream

`resource.type = 'cloud_dataproc_cluster'` is broad — it includes YARN, HDFS, Spark application logs, and audit logs from the same cluster. To target one specific stream, filter by `log_name`:

| Stream of interest | `log_name LIKE` pattern |
|---|---|
| Dataproc admin operations (CreateCluster, DeleteCluster) | `'%cloudaudit.googleapis.com%2Factivity'` |
| Autoscaler decisions (`AutoscalerLog` payload) | `'%dataproc.googleapis.com%2Fautoscaler'` |
| Cluster TTL reconciler (auto-delete events) | `'%dataproc.googleapis.com%2Fcluster_reconciler'` |
| Dataproc agent (job lifecycle notes) | `'%google.dataproc.agent'` |
| Spark / YARN / HDFS application logs | `'%hadoop-yarn-*'`, `'%hadoop-hdfs-*'`, `'%spark-history-server'`, `'%yarn-userlogs'` (noise for most lifecycle queries) |

Run a probe query first (see "Discovery probe" below) when adding a new SQL — log streams in this view evolve over time.

## Lookback window

Log Analytics retention here is approximately **24 hours**. A cluster that lived longer than that will show no explicit `CreateCluster` / `DeleteCluster` event in the window — its "first observed timestamp" will hug the lookback edge. b20 handles this with explicit-vs-fallback flags (`has_explicit_create`, `has_explicit_delete`); new queries that depend on lifecycle events should expose similar diagnostic columns rather than silently truncating.

## Audit logs — duplicate-event collapsing

For long-running operations (CreateCluster, DeleteCluster, UpdateCluster), GCP audit logs emit **at least two records** per operation: a "request received" entry and a "completion" entry, often 2–3 minutes apart. Both have the same `proto_payload.audit_log.method_name`. Treating each entry as a separate operation produces phantom incarnations.

The pairing pattern used in `b20_cluster_span_time.sql`:

1. Compute `prev_delete_ts` for each Create — the most recent Delete on the same cluster strictly before this Create.
2. `GROUP BY (cluster_name, prev_delete_ts)` — every Create within the same "run between Deletes" collapses; take `MIN(create_ts)` as canonical (when the API call was made = when billing started).
3. Same pattern for Deletes (group by previous Create).

If a future query needs operation-level uniqueness, prefer this run-anchor approach over time-windowed dedupe heuristics — it correctly handles legitimately rapid recreation cycles.

## Same-name across incarnations

`cluster_name` is **not unique** within the lookback window. The same name can be deleted and recreated multiple times in 24h (each Create→Delete cycle is a separate billing interval and should be a separate row). Always carry an `incarnation_idx` (or the cluster UUID, if available in the payload) when emitting per-incarnation rows.

For pairing audit Creates with Deletes:

```sql
-- For each Create, find the soonest Delete after it on the same cluster.
paired AS (
  SELECT
    c.cluster_name,
    c.create_ts,
    (SELECT MIN(d.delete_ts) FROM deletes d
      WHERE d.cluster_name = c.cluster_name
        AND d.delete_ts > c.create_ts) AS delete_ts
  FROM creates c
)
```

Plus orphan-delete detection (Deletes with no preceding Create — cluster was created before window start).

## Discovery probe template

When adding a new query against an unfamiliar log stream, run a probe first:

```sql
SELECT
  log_name,
  COUNT(*) AS n,
  ANY_VALUE(SUBSTR(CAST(COALESCE(text_payload, TO_JSON_STRING(json_payload), '') AS STRING), 1, 240)) AS sample_msg
FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
WHERE resource.type = 'cloud_dataproc_cluster'
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
GROUP BY log_name
ORDER BY n DESC
LIMIT 30;
```

Look for: `cloudaudit.googleapis.com` for admin actions, `google.cloud.dataproc.logging.*` payload `@type` for structured Dataproc logs.

For payload schema, dump 3 representative rows raw:

```sql
SELECT timestamp, JSON_VALUE(resource.labels.cluster_name) AS cluster_name,
       TO_JSON_STRING(json_payload) AS payload
FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
WHERE log_name LIKE '%dataproc.googleapis.com%2Fautoscaler'
  AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 MINUTE)
ORDER BY timestamp DESC
LIMIT 3;
```

Then read the JSON paths from the actual payload — never guess from naming conventions.

## CSV-export quirks

Queries land on disk via "Save query results → CSV" in the BigQuery console. Two consequences:

- **Timestamps**: BigQuery exports as `YYYY-MM-DD HH:MM:SS.SSSSSS UTC` (always UTC, microsecond precision). Loaders must accept this in addition to ISO-8601 — see `ClusterMachineAndRecipeTuner.parseInstant`.
- **Cluster name in `b14`**: BigQuery JSON-embeds the value, producing `"""cluster-name"""` (triple-quoted). The b14 loader strips all `"` chars after parsing. Don't try to "fix" this in the SQL — it's a property of how the export wraps JSON-derived strings.
- **Free-text fields with commas**: `Csv.parse` in this project splits on bare commas without honoring CSV-quoting. Avoid free-text columns with commas in new queries, or expect the loader to ignore those columns.

## Conventions for new bNN files

- Filename: `bNN_short_descriptive_name.sql` matching the pattern of neighboring files.
- Header comment: purpose, output columns (with types), source filters, validation gates, known caveats. Block at the top of the file.
- Use `JSON_VALUE` for any `resource.labels.*` extraction.
- Project explicit BOOL flags (`has_explicit_*`, `is_*`) rather than NULL checks downstream — the loader's job is then trivial.
- Order results by something stable (cluster_name + timestamp) so diffs across runs are reviewable.

## When you hit a new error

The errors most likely to bite:
- `Grouping by expressions of type JSON is not allowed` → use `JSON_VALUE` instead of `CAST(... AS STRING)`.
- `No matching signature for function JSON_VALUE: STRUCT<...>` → it's a STRUCT, use direct field access.
- `Cannot access field X on a value with type Y` → check whether the field exists in this schema variant; consider COALESCE across `snake_case` and `camelCase` (e.g. `proto_payload.audit_log.method_name` vs `methodName` — different exports use different conventions).
- Empty result set → log_name filter likely too strict; broaden and probe.
