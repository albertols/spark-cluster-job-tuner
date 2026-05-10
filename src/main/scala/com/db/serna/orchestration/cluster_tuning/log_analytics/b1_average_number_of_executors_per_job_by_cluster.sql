-- Reusable parsing CTEs: normalize log lines
-- Select your time range in the Log Analytics UI; this query respects that range.
WITH base AS (
  SELECT
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.timestamp_iso")),
      TIMESTAMP(timestamp)
    ) AS evt_ts,

    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.event_type") AS event_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") AS cluster_name,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.recipeFilename") AS recipe_filename,

    -- Executor status fields
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.executor_event") AS executor_event,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.current_no_executors") AS INT64) AS current_no_executors,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.total_no_executors") AS INT64) AS total_no_executors,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.max_executors_seen") AS INT64) AS max_executors_seen,

    -- Application-level fields
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_start_iso")) AS app_start_ts,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_end_iso"))   AS app_end_ts,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_duration_millis") AS INT64) AS app_duration_millis,

    -- If your logs contain applicationId, uncomment the line below and use it in joins
    -- JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.application_id") AS application_id
  FROM
    `your-project.global._Default._Default`
  WHERE
    resource.type = "cloud_dataproc_job"
    AND JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") IS NOT NULL
),
application_runs AS (
  SELECT
    cluster_name,
    recipe_filename,
    app_start_ts,
    app_end_ts,
    app_duration_millis,
    -- Use application_id if available; otherwise key by cluster+recipe+start
    CONCAT(cluster_name, '|', recipe_filename, '|', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', app_start_ts)) AS run_id
  FROM base
  WHERE event_type = "application_end"
    AND app_start_ts IS NOT NULL
    AND app_end_ts IS NOT NULL
),
executor_events AS (
  SELECT
    cluster_name,
    recipe_filename,
    evt_ts,
    executor_event,
    current_no_executors
  FROM base
  WHERE event_type = "executor_status"
),
-- DESCRIPTION EXPORT: Average number of executors used per job (recipeFilename) in a CLUSTER_NAME
-- Time-weighted average of current_no_executors over the job window.
events_in_runs AS (
  SELECT
    r.run_id,
    r.cluster_name,
    r.recipe_filename,
    r.app_start_ts,
    r.app_end_ts,
    r.app_duration_millis,
    e.evt_ts,
    e.current_no_executors
  FROM application_runs r
  JOIN executor_events e
    ON e.cluster_name = r.cluster_name
   AND e.recipe_filename = r.recipe_filename
   AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
),
segmented AS (
  SELECT
    run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts, app_duration_millis,
    evt_ts,
    current_no_executors,
    LEAD(evt_ts) OVER (PARTITION BY run_id ORDER BY evt_ts) AS next_evt_ts
  FROM events_in_runs
),
weighted AS (
  SELECT
    run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts, app_duration_millis,
    current_no_executors,
    GREATEST(
      0,
      TIMESTAMP_DIFF(
        LEAST(COALESCE(next_evt_ts, app_end_ts), app_end_ts),
        evt_ts,
        SECOND
      )
    ) AS seg_seconds
  FROM segmented
),
per_run_avg AS (
  SELECT
    run_id,
    cluster_name,
    recipe_filename,
    SAFE_DIVIDE(
      SUM(current_no_executors * seg_seconds),
      NULLIF(TIMESTAMP_DIFF(app_end_ts, app_start_ts, SECOND), 0)
    ) AS run_avg_executors
  FROM weighted
  GROUP BY run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts
)
SELECT
  cluster_name,
  recipe_filename,
  AVG(run_avg_executors) AS avg_executors_per_job
FROM per_run_avg
GROUP BY cluster_name, recipe_filename
ORDER BY avg_executors_per_job DESC;