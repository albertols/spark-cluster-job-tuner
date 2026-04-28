-- b20_cluster_span_time.sql
--
-- Purpose: emit one row per Dataproc cluster with the wall-clock span
-- (creation → destruction) so the autoscale-cost calc (b22/b23) can integrate
-- worker counts × time over a real interval, not over summed job durations.
--
-- Output columns:
--   cluster_name             STRING
--   span_start_ts            TIMESTAMP   -- earliest signal we have for this cluster
--   span_end_ts              TIMESTAMP   -- latest signal we have for this cluster
--   span_minutes             FLOAT64     -- (span_end_ts − span_start_ts) in minutes
--   create_event_ts          TIMESTAMP   -- best-effort timestamp of the explicit Create marker (NULL if not seen)
--   delete_event_ts          TIMESTAMP   -- best-effort timestamp of the explicit Delete marker (NULL if not seen)
--   has_explicit_create      BOOL
--   has_explicit_delete      BOOL
--   total_events             INT64       -- diagnostic: how many cluster-scoped events contributed to the span
--
-- Validation gate (please run this and report back):
--   1) Row count vs. expected number of clusters in the window.
--   2) For 2–3 known clusters: do span_start_ts / span_end_ts look right?
--   3) Are has_explicit_create / has_explicit_delete usually true? If a cluster
--      has neither, span_start_ts/span_end_ts will fall back to first/last
--      cluster-scoped log line — confirm that's an acceptable fallback.
--   4) The two CREATE/DELETE regexes (in `markers` CTE) are guesses based on
--      common Dataproc operation messages. Replace them with the actual log
--      patterns you see, then we can tighten this query.
--
-- Source project matches b1–b13/b15/b16 (per project decision: `db-prd-rn63-pwcclake-es`).

WITH base AS (
  SELECT
    timestamp,
    -- resource.labels.* is JSON-typed in Log Analytics views. CAST(... AS STRING)
    -- preserves the JSON type and is rejected by GROUP BY; JSON_VALUE extracts
    -- the scalar as a true STRING. (Same fix applies if you see the
    -- "Grouping by expressions of type JSON is not allowed" error elsewhere.)
    JSON_VALUE(resource.labels.cluster_name) AS cluster_name,
    CAST(COALESCE(text_payload, TO_JSON_STRING(json_payload), '') AS STRING) AS msg
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE resource.type = 'cloud_dataproc_cluster'
    AND JSON_VALUE(resource.labels.cluster_name) IS NOT NULL
),
markers AS (
  SELECT
    cluster_name,
    timestamp,
    -- TODO(validate): refine these regexes once we see real log payloads.
    -- These match common Dataproc operation messages.
    REGEXP_CONTAINS(msg, r'(?i)(creating cluster|cluster\s+created|CreateCluster\s+completed)') AS is_create,
    REGEXP_CONTAINS(msg, r'(?i)(deleting cluster|cluster\s+deleted|DeleteCluster\s+completed)') AS is_delete
  FROM base
),
spans AS (
  SELECT
    cluster_name,
    MIN(timestamp)                                                     AS span_start_ts,
    MAX(timestamp)                                                     AS span_end_ts,
    MIN(IF(is_create, timestamp, NULL))                                AS create_event_ts,
    MAX(IF(is_delete, timestamp, NULL))                                AS delete_event_ts,
    LOGICAL_OR(is_create)                                              AS has_explicit_create,
    LOGICAL_OR(is_delete)                                              AS has_explicit_delete,
    COUNT(*)                                                           AS total_events
  FROM markers
  GROUP BY cluster_name
)
SELECT
  cluster_name,
  -- Prefer explicit markers when present; otherwise fall back to first/last event.
  COALESCE(create_event_ts, span_start_ts) AS span_start_ts,
  COALESCE(delete_event_ts, span_end_ts)   AS span_end_ts,
  TIMESTAMP_DIFF(
    COALESCE(delete_event_ts, span_end_ts),
    COALESCE(create_event_ts, span_start_ts),
    SECOND
  ) / 60.0                                  AS span_minutes,
  create_event_ts,
  delete_event_ts,
  has_explicit_create,
  has_explicit_delete,
  total_events
FROM spans
ORDER BY span_minutes DESC;
