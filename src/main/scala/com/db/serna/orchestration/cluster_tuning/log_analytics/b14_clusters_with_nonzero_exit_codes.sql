-- =============================================================================
-- b14_clusters_with_nonzero_exit_codes.sql
--
-- Purpose:    Clusters with non-zero job exit codes — driver eviction signal
--             feeding the boost lifecycle (RefinementVitamins).
-- Telemetry:  GCP-native
-- GCP source: resource.type='cloud_dataproc_cluster' (Dataproc cluster
--             lifecycle + job exit-code events)
-- App source: n/a
-- Consumed:   ClusterDiagnostics.loadExitCodes
-- =============================================================================

WITH base AS (
  SELECT
    timestamp,
    resource.labels.cluster_name AS cluster_name,
    -- Force msg to STRING
    CAST(COALESCE(text_payload, TO_JSON_STRING(json_payload), '') AS STRING) AS msg
  FROM `db-dev-apyd-pwcclake-es.global._Default._Default`
  WHERE resource.type = 'cloud_dataproc_cluster'
),
parsed AS (
  SELECT
    timestamp,
    cluster_name,
    REGEXP_EXTRACT(msg, r"Job\s+'([^']+)'") AS job_id,
    COALESCE(
      SAFE_CAST(REGEXP_EXTRACT(msg, r"driver_exit_code:\s*([0-9]+)") AS INT64),
      SAFE_CAST(REGEXP_EXTRACT(msg, r"exit code\s+([0-9]+)") AS INT64)
    ) AS driver_exit_code,
    msg
  FROM base
  WHERE (msg LIKE '%completed with exit code%' OR msg LIKE '%driver_exit_code:%')
)
SELECT
  timestamp,
  job_id,
  cluster_name,
  driver_exit_code,
  msg
FROM parsed
WHERE job_id IS NOT NULL
  AND driver_exit_code IS NOT NULL
  AND driver_exit_code != 0
ORDER BY timestamp DESC;