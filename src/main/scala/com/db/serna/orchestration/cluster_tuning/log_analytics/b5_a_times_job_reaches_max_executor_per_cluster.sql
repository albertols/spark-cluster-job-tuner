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
-- Number of times a job (recipeFilename) in a CLUSTER_NAME reached the maximum number of executors
-- Provide a static per-cluster executor cap (good when you know executor_cores and memory footprint)
-- sets cap to 12 for a cluster (6 workers × 2 executors/worker)
-- User-provided/maintained capacity table (edit to your reality)
capacity AS (
SELECT "cluster-wf-carga-act-bfbe08c0-main" AS cluster_name, 12 AS max_exec_allowed UNION ALL
SELECT "cluster-wf-load-info-83c7f70d-0730", 12 UNION ALL
SELECT "loans-and-leasing-527-1", 12
),

job_run_peak AS (
SELECT
r.run_id,
r.cluster_name,
r.recipe_filename,
MAX(e.max_executors_seen) AS run_max_executors
FROM application_runs r
JOIN executor_events e
ON e.cluster_name = r.cluster_name
AND e.recipe_filename = r.recipe_filename
AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
GROUP BY r.run_id, r.cluster_name, r.recipe_filename
)

SELECT
p.cluster_name,
p.recipe_filename,
SUM(CASE WHEN p.run_max_executors >= c.max_exec_allowed THEN 1 ELSE 0 END) AS runs_reaching_cap,
COUNT(*) AS total_runs,
SAFE_DIVIDE(
SUM(CASE WHEN p.run_max_executors >= c.max_exec_allowed THEN 1 ELSE 0 END),
COUNT(*)
) AS fraction_reaching_cap
FROM job_run_peak p
JOIN capacity c
ON c.cluster_name = p.cluster_name
GROUP BY p.cluster_name, p.recipe_filename
ORDER BY runs_reaching_cap DESC, total_runs DESC;