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
-- DESCRIPTION EXPORT: Total runtime of all jobs in a CLUSTER_NAME (within selected window)
SELECT
cluster_name,
SUM(app_duration_millis) AS total_runtime_ms,
ROUND(SUM(app_duration_millis)/1000/60, 2) AS total_runtime_minutes,
ROUND(SUM(app_duration_millis)/1000/60/60, 2) AS total_runtime_hours
FROM application_runs
GROUP BY cluster_name
ORDER BY total_runtime_hours DESC;
