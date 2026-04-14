/*
Purpose:
This query analyzes Dataproc application end events and execution status logs
to identify the latest status per `cluster_name` and `recipe_filename`,
together with the average application duration.

Main goal:
Highlight recipes that have a recent application run but may have a missing
or null execution status when joined with status logs.

Data sources:
- Application end events parsed from log payloads
- Execution status events parsed from free-text log messages

Typical usage:
- Investigate recipes whose latest status is null
- Compare latest Dataproc `job_id` with execution status `app_id`
- Support cluster tuning and operational troubleshooting
*/

WITH app_base AS (
  -- Parse application_end from EventListener
  --\{"event_type":"application_end","event_type_family":"SCALAMATICA-DWH_TO_CDM","clusterName":"cluster-wf-dmr-load-t-04-35-0435","recipeFilename":"_ETL_m_DM_LKP_COSTE_INSTR_FI.json","layerToLayer":"DWH_TO_CDM","version":"0.6.6","app_start_iso":"2026-03-30T08:12:22.644Z","app_end_iso":"2026-03-30T09:27:07.238Z","app_duration_millis":"4484594"\}
  SELECT
    COALESCE(
      TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.timestamp_iso")),
      TIMESTAMP(timestamp)
    ) AS evt_ts,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.event_type") AS event_type,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") AS cluster_name,
    JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.recipeFilename") AS recipe_filename,
    JSON_EXTRACT_SCALAR(resource.labels, "$.job_id") AS job_id,
    SAFE_CAST(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_duration_millis") AS INT64) AS app_duration_millis,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_start_iso")) AS app_start_ts,
    TIMESTAMP(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.app_end_iso")) AS app_end_ts
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE
    resource.type = "cloud_dataproc_job"
    AND JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), "$.clusterName") IS NOT NULL
),
application_runs AS (
  SELECT
    cluster_name,
    recipe_filename,
    job_id,
    app_duration_millis,
    app_start_ts,
    evt_ts
  FROM app_base
  WHERE event_type = "application_end"
    AND app_start_ts IS NOT NULL
    AND app_end_ts IS NOT NULL
    AND recipe_filename IS NOT NULL
),
avg_durations AS (
  SELECT
    cluster_name,
    recipe_filename,
    AVG(app_duration_millis) AS avg_job_duration_ms
  FROM application_runs
  GROUP BY cluster_name, recipe_filename
),
latest_app_job AS (
  SELECT
    cluster_name,
    recipe_filename,
    job_id,
    app_start_ts
  FROM (
    SELECT
      cluster_name,
      recipe_filename,
      job_id,
      app_start_ts,
      ROW_NUMBER() OVER (
        PARTITION BY cluster_name, recipe_filename
        ORDER BY evt_ts DESC
      ) AS rn
    FROM application_runs
  )
  WHERE rn = 1
),
status_base AS (
  --Extracting execution status JSON:
  --"Execution status: {"CLUSTER_NAME":"cluster-wf-dmr-load-t-04-35-0435","LAYER":"DWH_TO_CDM","BUSINESS_DATE":"2026-03-29","JOB_ID":"application_1774841886808_0243","RECIPE":"_ETL_m_DM_LKP_COSTE_INSTR_FI.json","NAR_ID":"","SOURCE_TABLES":"DWH_LKP_COSTE_INSTR_FI","STATUS":"SUCCESS","MESSAGE":"","COUNT":3829507179,"CONTROL_ERRORS_COUNT":0,"START_TIME":"2026-03-30T08:12:20.000Z","END_TIME":"2026-03-30T09:27:00.000Z","DURATION":"1h 14m 40sec","ARGUMENTS":"--executionMode=GCP --env=prd --sourceIO=BQ --targetIO=BQ --projectVersion=0.6.6 --layer=DWH_TO_CDM --parallelizationFactor=5
  --recipeFilename=_ETL_m_DM_LKP_COSTE_INSTR_FI.json --clusterName=cluster-wf-dmr-load-t-04-35-0435 --partitionDateFormat=yyyy-MM-dd --businessDate=2026-03-29 --scheduleTime=2026-03-29T19:30:00+00:00","SCHEDULE_TIME":"2026-03-29T19:30:00.000Z","TARGET_TABLE":"LKP_COSTE_INSTR_FI"}"
  SELECT
    TIMESTAMP(timestamp) AS log_ts,
    JSON_EXTRACT(
      REGEXP_EXTRACT(
        JSON_EXTRACT_SCALAR(json_payload, "$.message"),
        r'.*Execution status:\s*(\{.*\})'
      ),
      "$"
    ) AS parsed_json
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE
    resource.type = "cloud_dataproc_job"
    AND CONTAINS_SUBSTR(JSON_EXTRACT_SCALAR(json_payload, "$.message"), 'Execution status: {')
),
status_events AS (
  SELECT
    log_ts,
    JSON_EXTRACT_SCALAR(parsed_json, "$.CLUSTER_NAME") AS cluster_name,
    JSON_EXTRACT_SCALAR(parsed_json, "$.RECIPE") AS recipe_filename,
    JSON_EXTRACT_SCALAR(parsed_json, "$.JOB_ID") AS app_id,
    JSON_EXTRACT_SCALAR(parsed_json, "$.STATUS") AS status,
    JSON_EXTRACT_SCALAR(parsed_json, "$.MESSAGE") AS message
  FROM status_base
),
status_ranked AS (
  SELECT
    cluster_name,
    recipe_filename,
    app_id,
    status,
    message,
    ROW_NUMBER() OVER (
      PARTITION BY cluster_name, recipe_filename
      ORDER BY log_ts DESC
    ) AS rn
  FROM status_events
),
status_per_recipe AS (
  SELECT
    cluster_name,
    recipe_filename,
    app_id,
    status,
    message
  FROM status_ranked
  WHERE rn = 1
)
SELECT
  d.cluster_name,
  d.recipe_filename,
  a.job_id,
  a.app_start_ts AS app_start_iso,
  --s.app_id,
  CONCAT(
    CAST(DIV(CAST(ROUND(d.avg_job_duration_ms / 1000.0) AS INT64), 60) AS STRING),
    'm ',
    CAST(MOD(CAST(ROUND(d.avg_job_duration_ms / 1000.0) AS INT64), 60) AS STRING),
    'sec'
  ) AS avg_job_duration_in_mins_sec,
  s.status,
  s.message
FROM avg_durations d
LEFT JOIN latest_app_job a
  ON d.cluster_name = a.cluster_name
 AND d.recipe_filename = a.recipe_filename
LEFT JOIN status_per_recipe s
  ON d.cluster_name = s.cluster_name
 AND d.recipe_filename = s.recipe_filename
ORDER BY d.avg_job_duration_ms DESC;