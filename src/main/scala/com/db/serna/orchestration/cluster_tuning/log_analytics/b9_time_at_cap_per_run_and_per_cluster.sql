-- Reusable parsing CTEs: normalize log lines
-- Select your time range in the Log Analytics UI; this query respects that range.
WITH base AS (
  SELECT
    -- The log entry timestamp; prefer message's own ISO when provided
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
    current_no_executors,
    total_no_executors,
    max_executors_seen
  FROM base
  WHERE event_type = "executor_status"
),
-- DESCRIPTION EXPORT: Time at cap per run and per cluster
-- Uses the same capacity table; sums seconds where current_no_executors >= cap.
capacity AS (
  SELECT "cluster-wf-load-info-83c7f70d-0730" AS cluster_name, 12 AS max_exec_allowed
),
events_in_runs AS (
  SELECT r.run_id, r.cluster_name, r.recipe_filename, r.app_start_ts, r.app_end_ts,
         e.evt_ts, e.current_no_executors
  FROM application_runs r
  JOIN executor_events e
    ON e.cluster_name = r.cluster_name
   AND e.recipe_filename = r.recipe_filename
   AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
),
segmented AS (
  SELECT
    run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts,
    evt_ts, current_no_executors,
    LEAD(evt_ts) OVER (PARTITION BY run_id ORDER BY evt_ts) AS next_evt_ts
  FROM events_in_runs
),
dur AS (
  SELECT
    s.run_id, s.cluster_name, s.recipe_filename,
    GREATEST(
      0,
      TIMESTAMP_DIFF(LEAST(COALESCE(s.next_evt_ts, s.app_end_ts), s.app_end_ts), s.evt_ts, SECOND)
    ) AS seg_seconds,
    s.current_no_executors
  FROM segmented s
)
SELECT
  d.cluster_name,
  d.recipe_filename,
  SUM(CASE WHEN d.current_no_executors >= c.max_exec_allowed THEN d.seg_seconds ELSE 0 END) AS seconds_at_cap
FROM dur d
JOIN capacity c
  ON c.cluster_name = d.cluster_name
GROUP BY d.cluster_name, d.recipe_filename
ORDER BY seconds_at_cap DESC;