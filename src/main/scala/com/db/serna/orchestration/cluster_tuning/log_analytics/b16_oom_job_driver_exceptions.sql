-- =============================================================================
-- b16_oom_job_driver_exceptions.sql
--
-- Purpose:    Driver-side OOM events per recipe — feeds the heap-boost
--             vitamin (Holding / ReBoost lifecycle).
-- Telemetry:  both
-- GCP source: resource.type='cloud_dataproc_job' (Dataproc driver log stream;
--             log_name: dataproc.job.driver — where Spark driver OOM
--             stacktraces land)
-- App source: ExecutorTrackingListener (driver OOM correlation —
--             same listener as b13)
-- Consumed:   ClusterMachineAndRecipeTunerRefinement (MemoryHeapBoostVitamin —
--             primary single-tuner consumer);
--             ClusterMachineAndRecipeAutoTuner (b16 boost classification)
-- =============================================================================

/*
Purpose:
This query analyzes Dataproc driver logs to retrieve the latest relevant
driver message per `job_id`, with emphasis on exception/error patterns.

Main goal:
Surface jobs ending with Java heap `OutOfMemoryError` and enrich each row
with the related `recipe_filename` from application_end events.

Data sources:
- Dataproc driver log entries (`dataproc.job.driver`)
- EventListener application_end payloads (for recipe resolution)

Typical usage:
- Investigate OOM failures at driver level
- Correlate `job_id` with recipe execution
- Support cluster tuning and operational troubleshooting
*/

WITH driver_logs AS (
  -- Parse relevant driver logs and classify common exception signals.
  SELECT
    TIMESTAMP(timestamp) AS log_ts,
    JSON_EXTRACT_SCALAR(resource.labels, "$.job_id") AS job_id,
    REGEXP_REPLACE(
      COALESCE(
        JSON_VALUE(labels.`dataproc.googleapis.com/cluster_name`),
        JSON_VALUE(resource.labels, "$.cluster_name")
      ),
      r'^"|"$',
      ''
    ) AS cluster_name,
    JSON_EXTRACT_SCALAR(json_payload, "$.class") AS log_class,
    JSON_EXTRACT_SCALAR(json_payload, "$.message") AS driver_message,
    severity,
    log_name,
    REGEXP_EXTRACT(
      JSON_EXTRACT_SCALAR(json_payload, "$.message"),
      r'(java\.[A-Za-z0-9_$.]+(?:Exception|Error))'
    ) AS exception_type,
    REGEXP_CONTAINS(JSON_EXTRACT_SCALAR(json_payload, "$.message"), r'^Lost task ') AS is_lost_task,
    REGEXP_CONTAINS(JSON_EXTRACT_SCALAR(json_payload, "$.message"), r'StackOverflowError') AS is_stack_overflow,
    REGEXP_CONTAINS(
      JSON_EXTRACT_SCALAR(json_payload, "$.message"),
      r'java\.lang\.OutOfMemoryError: Java heap space'
    ) AS is_java_heap
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE
    resource.type = "cloud_dataproc_job"
    AND log_name = "projects/db-prd-rn63-pwcclake-es/logs/dataproc.job.driver"
    AND JSON_EXTRACT_SCALAR(resource.labels, "$.job_id") IS NOT NULL
    AND (
      severity IN ("WARNING", "ERROR", "CRITICAL", "ALERT", "EMERGENCY")
      OR REGEXP_CONTAINS(JSON_EXTRACT_SCALAR(json_payload, "$.message"), r'(Exception|Error|OutOfMemory)')
    )
),
latest_driver_log AS (
  -- Keep only the latest driver log per Dataproc job_id.
  SELECT
    job_id,
    cluster_name,
    log_ts,
    severity,
    log_class,
    exception_type,
    is_lost_task,
    is_stack_overflow,
    is_java_heap,
    driver_message,
    log_name
  FROM (
    SELECT
      job_id,
      cluster_name,
      log_ts,
      severity,
      log_class,
      exception_type,
      is_lost_task,
      is_stack_overflow,
      is_java_heap,
      driver_message,
      log_name,
      ROW_NUMBER() OVER (
        PARTITION BY job_id
        ORDER BY log_ts DESC
      ) AS rn
    FROM driver_logs
  )
  WHERE rn = 1
),
app_base AS (
  -- Parse EventListener payload to map job_id -> latest recipeFilename.
  SELECT
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.timestamp_iso")),
      TIMESTAMP(timestamp)
    ) AS evt_ts,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.event_type") AS event_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.recipeFilename") AS recipe_filename,
    JSON_EXTRACT_SCALAR(resource.labels, "$.job_id") AS job_id
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE resource.type = "cloud_dataproc_job"
),
latest_recipe_per_job AS (
  -- Resolve one recipe per job_id based on latest application_end event.
  SELECT
    job_id,
    recipe_filename
  FROM (
    SELECT
      job_id,
      recipe_filename,
      ROW_NUMBER() OVER (
        PARTITION BY job_id
        ORDER BY evt_ts DESC
      ) AS rn
    FROM app_base
    WHERE event_type = "application_end"
      AND recipe_filename IS NOT NULL
      AND job_id IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  d.job_id,
  d.cluster_name,
  r.recipe_filename,
  d.log_ts AS latest_driver_log_ts,
  d.severity AS latest_driver_log_severity,
  d.log_class AS latest_driver_log_class,
  d.exception_type AS latest_driver_exception_type,
  d.is_lost_task,
  d.is_stack_overflow,
  d.is_java_heap,
  d.driver_message AS latest_driver_message,
  d.log_name
FROM latest_driver_log d
LEFT JOIN latest_recipe_per_job r
  ON d.job_id = r.job_id
WHERE d.is_java_heap = TRUE
ORDER BY d.log_ts DESC;
