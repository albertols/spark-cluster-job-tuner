-- b21_cluster_autoscaler_values.sql
--
-- Purpose: emit ONE ROW PER AUTOSCALER EVENT carrying primary / secondary worker
-- counts so the autoscale-cost calc (b22/b23) can integrate workers x time over
-- each cluster incarnation produced by b20.
--
-- Output columns:
--   cluster_name                STRING
--   event_ts                    TIMESTAMP    -- when the autoscaler recorded this event
--   state                       STRING       -- e.g. RECOMMENDING / COOLDOWN
--   decision                    STRING       -- NO_SCALE / SCALE_UP / SCALE_DOWN (NULL outside RECOMMENDING)
--   decision_metric             STRING       -- e.g. YARN_MEMORY (NULL outside RECOMMENDING)
--   current_primary_workers     INT64        -- inputs.currentClusterSize.primaryWorkerCount
--   target_primary_workers      INT64        -- outputs.recommendedClusterSize.primaryWorkerCount
--   min_primary_workers         INT64        -- inputs.minWorkerCounts.primaryWorkerCount
--   max_primary_workers         INT64        -- inputs.maxWorkerCounts.primaryWorkerCount
--   current_secondary_workers   INT64        -- inputs.currentClusterSize.secondaryWorkerCount    (NULL if not used)
--   target_secondary_workers    INT64        -- outputs.recommendedClusterSize.secondaryWorkerCount (NULL if not used)
--   min_secondary_workers       INT64
--   max_secondary_workers       INT64
--   recommendation_id           STRING
--   status_details              STRING       -- free-text status message (e.g. "2 minute cooldown started ...")
--
-- Cost-integration intent (consumer is Scala b22):
--   * Filter to RECOMMENDING events (target_* IS NOT NULL).
--   * Sort per cluster_name by event_ts ASC.
--   * For each interval [event_ts_i, event_ts_{i+1}), assume the cluster ran with
--     target_primary_workers (+ target_secondary_workers if non-NULL). The first
--     interval extends back to the b20 span_start_ts; the last interval extends
--     forward to the b20 span_end_ts. When a (cluster_name, span) has zero
--     RECOMMENDING events, b23 fallback applies (avg of min/max or static workers).
--
-- Notes:
--   * json_payload is JSON-typed, so JSON_VALUE extracts scalars by path.
--   * Same-name across incarnations: this query does NOT carry incarnation_idx;
--     Scala matches autoscaler events to b20 incarnations by event_ts BETWEEN
--     span_start_ts AND span_end_ts (b20 incarnations don't overlap on a single
--     cluster_name by construction).

WITH autoscaler AS (
  SELECT
    timestamp,
    JSON_VALUE(resource.labels.cluster_name) AS cluster_name,
    json_payload                              AS payload
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE log_name LIKE '%dataproc.googleapis.com%2Fautoscaler'
    AND JSON_VALUE(resource.labels.cluster_name) IS NOT NULL
)
SELECT
  cluster_name,
  timestamp AS event_ts,
  JSON_VALUE(payload, '$.status.state')                                                       AS state,
  JSON_VALUE(payload, '$.recommendation.outputs.decision')                                    AS decision,
  JSON_VALUE(payload, '$.recommendation.outputs.decisionMetric')                              AS decision_metric,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.currentClusterSize.primaryWorkerCount')      AS INT64) AS current_primary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.outputs.recommendedClusterSize.primaryWorkerCount') AS INT64) AS target_primary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.minWorkerCounts.primaryWorkerCount')         AS INT64) AS min_primary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.maxWorkerCounts.primaryWorkerCount')         AS INT64) AS max_primary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.currentClusterSize.secondaryWorkerCount')      AS INT64) AS current_secondary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.outputs.recommendedClusterSize.secondaryWorkerCount') AS INT64) AS target_secondary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.minWorkerCounts.secondaryWorkerCount')         AS INT64) AS min_secondary_workers,
  SAFE_CAST(JSON_VALUE(payload, '$.recommendation.inputs.maxWorkerCounts.secondaryWorkerCount')         AS INT64) AS max_secondary_workers,
  JSON_VALUE(payload, '$.recommendation.outputs.recommendationId')                            AS recommendation_id,
  JSON_VALUE(payload, '$.status.details')                                                     AS status_details
FROM autoscaler
ORDER BY cluster_name, event_ts;
