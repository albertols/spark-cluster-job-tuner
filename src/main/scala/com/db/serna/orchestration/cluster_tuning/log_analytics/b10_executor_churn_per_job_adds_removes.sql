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
`$(gcp_project_id).global._Default._Default`
EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") IS NOT NULL
%M:%E*S%Ez', app_start_ts)) AS run_id
-- DESCRIPTION EXPORT: Executor churn per job (adds/removes), a proxy for allocation instability
labeled AS (
SELECT
r.run_id, r.cluster_name, r.recipe_filename,
e.executor_event
FROM application_runs r
JOIN executor_events e
ON e.cluster_name = r.cluster_name
AND e.recipe_filename = r.recipe_filename
AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
WHERE e.executor_event IN ("EXECUTOR_ADDED","EXECUTOR_REMOVED")
)
SELECT
cluster_name,
recipe_filename,
SUM(CASE WHEN executor_event = "EXECUTOR_ADDED" THEN 1 ELSE 0 END) AS executors_added,
SUM(CASE WHEN executor_event = "EXECUTOR_REMOVED" THEN 1 ELSE 0 END) AS executors_removed
FROM labeled
GROUP BY cluster_name, recipe_filename
ORDER BY executors_added + executors_removed DESC;