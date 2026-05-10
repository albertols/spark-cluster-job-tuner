-- =============================================================================
-- b13_recommendations_inputs_per_recipe_per_cluster.sql
--
-- Purpose:    Per-recipe / per-cluster baseline metrics (avg, p95 duration,
--             max executors, run counts) — primary input to SingleTuner +
--             AutoTuner cluster sizing.
-- Telemetry:  both
-- GCP source: resource.type='cloud_dataproc_job' (Spark application log stream
--             — where executor lifecycle events surface, including those
--             emitted by ExecutorTrackingListener)
-- App source: ExecutorTrackingListener (executor lifecycle events emitted
--             from your Spark application — wire it via
--             spark.extraListeners=com.db.serna.utils.spark.parallelism.ExecutorTrackingListener)
-- Consumed:   ClusterMachineAndRecipeTuner (single tuner load path)
-- =============================================================================

-- One-stop input for tuner: per (cluster, recipe)
-- Includes: avg executors per job, p95 run max executors, avg and p95 durations,
-- fraction reaching cap (requires capacity table), seconds at cap, total runs,
-- cluster max concurrent jobs
WITH base AS (
  SELECT
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.timestamp_iso")),
      TIMESTAMP(timestamp)
    ) AS evt_ts,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.event_type") AS event_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") AS cluster_name,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.recipeFilename") AS recipe_filename,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.executor_event") AS executor_event,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.current_no_executors") AS INT64) AS current_no_executors,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.max_executors_seen") AS INT64) AS max_executors_seen,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_start_iso")) AS app_start_ts,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_end_iso"))   AS app_end_ts,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_duration_millis") AS INT64) AS app_duration_millis
  FROM `your-project.global._Default._Default`
  WHERE resource.type = "cloud_dataproc_job"
    AND JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") IS NOT NULL
),
application_runs AS (
  SELECT
    cluster_name, recipe_filename, app_start_ts, app_end_ts, app_duration_millis,
    CONCAT(cluster_name, '|', recipe_filename, '|', FORMAT_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S%Ez', app_start_ts)) AS run_id
  FROM base
  WHERE event_type = "application_end"
    AND app_start_ts IS NOT NULL AND app_end_ts IS NOT NULL
),
executor_events AS (
  SELECT cluster_name, recipe_filename, evt_ts, executor_event, current_no_executors, max_executors_seen
  FROM base
  WHERE event_type = "executor_status"
),
events_in_runs AS (
  -- LEFT JOIN so application_end runs without matching executor_status events are preserved
  -- (they contribute to duration metrics and run counts but have NULL executor metrics).
  -- Using INNER JOIN here previously caused b13 to under-report runs and skew duration metrics
  -- versus the individual b1/b3/b8/b12 CSVs.
  SELECT r.run_id, r.cluster_name, r.recipe_filename, r.app_start_ts, r.app_end_ts, r.app_duration_millis,
         e.evt_ts, e.executor_event, e.current_no_executors, e.max_executors_seen
  FROM application_runs r
  LEFT JOIN executor_events e
    ON e.cluster_name = r.cluster_name
   AND e.recipe_filename = r.recipe_filename
   AND e.evt_ts BETWEEN r.app_start_ts AND r.app_end_ts
),
segmented AS (
  SELECT
    run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts, app_duration_millis,
    evt_ts, executor_event, current_no_executors, max_executors_seen,
    LEAD(evt_ts) OVER (PARTITION BY run_id ORDER BY evt_ts) AS next_evt_ts
  FROM events_in_runs
),
weighted AS (
  SELECT
    run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts, app_duration_millis,
    executor_event, current_no_executors, max_executors_seen,
    GREATEST(0, TIMESTAMP_DIFF(LEAST(COALESCE(next_evt_ts, app_end_ts), app_end_ts), evt_ts, SECOND)) AS seg_seconds
  FROM segmented
),
per_run AS (
  SELECT
    run_id,
    cluster_name,
    recipe_filename,
    app_start_ts,
    app_end_ts,
    SAFE_DIVIDE(
      SUM(current_no_executors * seg_seconds),
      NULLIF(TIMESTAMP_DIFF(app_end_ts, app_start_ts, SECOND), 0)
    ) AS run_avg_executors,
    MAX(max_executors_seen) AS run_max_executors,
    ANY_VALUE(app_duration_millis) AS run_duration_ms
  FROM weighted
  GROUP BY run_id, cluster_name, recipe_filename, app_start_ts, app_end_ts
),
agg AS (
  SELECT
    cluster_name,
    recipe_filename,
    AVG(run_avg_executors) AS avg_executors_per_job,
    APPROX_QUANTILES(run_max_executors, 100)[OFFSET(95)] AS p95_run_max_executors,
    AVG(run_duration_ms) AS avg_job_duration_ms,
    APPROX_QUANTILES(run_duration_ms, 100)[OFFSET(95)] AS p95_job_duration_ms,
    COUNT(*) AS runs
  FROM per_run
  GROUP BY cluster_name, recipe_filename
),
capacity AS (
  -- Optional: per-cluster capacity for "at cap" metrics
  -- Edit to your reality, or LEFT JOIN will yield NULLs for these fields.
  SELECT "cluster-wf-carga-act-bfbe08c0-main" AS cluster_name, 12 AS max_exec_allowed
),
time_at_cap AS (
  SELECT
    w.cluster_name, w.recipe_filename,
    SUM(CASE WHEN c.max_exec_allowed IS NOT NULL AND w.current_no_executors >= c.max_exec_allowed THEN w.seg_seconds ELSE 0 END) AS seconds_at_cap
  FROM weighted w
  JOIN capacity c USING (cluster_name)
  GROUP BY w.cluster_name, w.recipe_filename
),
runs_reaching_cap AS (
  SELECT
    p.cluster_name, p.recipe_filename,
    SUM(CASE WHEN p.run_max_executors >= c.max_exec_allowed THEN 1 ELSE 0 END) AS runs_reaching_cap,
    COUNT(*) AS total_runs
  FROM per_run p
  JOIN capacity c USING (cluster_name)
  GROUP BY p.cluster_name, p.recipe_filename
),
-- Approximate max concurrent jobs per cluster without window functions
cluster_time_bounds AS (
  SELECT
    cluster_name,
    MIN(app_start_ts) AS min_start,
    MAX(app_end_ts)   AS max_end
  FROM application_runs
  GROUP BY cluster_name
),
minutes AS (
  SELECT
    cluster_name,
    GENERATE_TIMESTAMP_ARRAY(
      TIMESTAMP_TRUNC(min_start, MINUTE),
      TIMESTAMP_TRUNC(max_end,   MINUTE),
      INTERVAL 1 MINUTE
    ) AS minute_bins
  FROM cluster_time_bounds
),
expanded AS (
  SELECT m.cluster_name, mb AS minute_ts
  FROM minutes m, UNNEST(m.minute_bins) AS mb
),
active AS (
  SELECT
    e.cluster_name,
    e.minute_ts,
    COUNTIF(e.minute_ts BETWEEN r.app_start_ts AND r.app_end_ts) AS concurrent_jobs
  FROM expanded e
  JOIN application_runs r
    ON r.cluster_name = e.cluster_name
  GROUP BY e.cluster_name, e.minute_ts
),
concurrency AS (
  SELECT
    cluster_name,
    MAX(concurrent_jobs) AS max_concurrent_jobs
  FROM active
  GROUP BY cluster_name
)
SELECT
  a.cluster_name,
  a.recipe_filename,
  a.avg_executors_per_job,
  a.p95_run_max_executors,
  a.avg_job_duration_ms,
  a.p95_job_duration_ms,
  a.runs,
  t.seconds_at_cap,
  r.runs_reaching_cap,
  r.total_runs,
  SAFE_DIVIDE(r.runs_reaching_cap, r.total_runs) AS fraction_reaching_cap,
  c.max_concurrent_jobs
FROM agg a
LEFT JOIN time_at_cap t USING (cluster_name, recipe_filename)
LEFT JOIN runs_reaching_cap r USING (cluster_name, recipe_filename)
LEFT JOIN concurrency c USING (cluster_name)
ORDER BY a.cluster_name, a.recipe_filename;