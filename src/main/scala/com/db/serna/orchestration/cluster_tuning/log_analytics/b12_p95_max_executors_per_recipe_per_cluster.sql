-- P95 of run-level max executors per recipe per cluster
WITH base AS (
  SELECT
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.timestamp_iso")),
      TIMESTAMP(timestamp)
    ) AS evt_ts,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.event_type") AS event_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") AS cluster_name,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.recipeFilename") AS recipe_filename,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.max_executors_seen") AS INT64) AS max_executors_seen,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_start_iso")) AS app_start_ts,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_end_iso"))   AS app_end_ts
  FROM `your-project.global._Default._Default`
  WHERE resource.type = "cloud_dataproc_job"
    AND JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") IS NOT NULL
),
application_runs AS (
  SELECT
    cluster_name, recipe_filename, app_start_ts, app_end_ts,
    CONCAT(cluster_name, '|', recipe_filename, '|', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', app_start_ts)) AS run_id
  FROM base
  WHERE event_type = "application_end"
    AND app_start_ts IS NOT NULL AND app_end_ts IS NOT NULL
),
executor_events AS (
  SELECT
    cluster_name, recipe_filename, evt_ts, max_executors_seen
  FROM base
  WHERE event_type = "executor_status"
),
-- DESCRIPTION EXPORT: P95 of run-level max executors per recipe per cluster Useful to set manual spark.executor.instances and DA max.
run_peaks AS (
  SELECT
    r.run_id, r.cluster_name, r.recipe_filename,
    MAX(e.max_executors_seen) AS run_max_executors
  FROM application_runs r
  JOIN executor_events e
    ON e.cluster_name = r.cluster_name
   AND e.recipe_filename = r.recipe_filename
   AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
  GROUP BY r.run_id, r.cluster_name, r.recipe_filename
)
SELECT
  cluster_name,
  recipe_filename,
  APPROX_QUANTILES(run_max_executors, 100)[OFFSET(95)] AS p95_run_max_executors,
  AVG(run_max_executors) AS avg_run_max_executors,
  COUNT(*) AS runs
FROM run_peaks
GROUP BY cluster_name, recipe_filename
ORDER BY p95_run_max_executors DESC, runs DESC;