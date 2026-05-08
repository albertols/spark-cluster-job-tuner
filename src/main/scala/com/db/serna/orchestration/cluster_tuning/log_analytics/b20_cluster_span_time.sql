-- =============================================================================
-- b20_cluster_span_time.sql
--
-- Purpose:    Per-cluster wall-clock active window (from / to timestamps)
--             — required for cluster cost computation and per-recipe
--             attribution windows.
-- Telemetry:  GCP-native
-- GCP source: resource.type='cloud_dataproc_cluster'
-- App source: n/a
-- Consumed:   ClusterMachineAndRecipeTuner (cost computation path)
-- =============================================================================

-- b20_cluster_span_time.sql
--
-- Purpose: emit ONE ROW PER CLUSTER INCARNATION (Create -> Delete cycle)
-- so the autoscale-cost calc (b22/b23) can integrate worker counts x time
-- over a real billing interval. Same `cluster_name` recreated multiple times
-- in the window produces multiple rows — uniqueness is (cluster_name, span_start_ts).
--
-- Output columns:
--   cluster_name             STRING
--   incarnation_idx          INT64       -- 1..N within the window for this cluster_name (chronological)
--   span_start_ts            TIMESTAMP   -- explicit CreateCluster ts when seen, else first observed event
--   span_end_ts              TIMESTAMP   -- explicit DeleteCluster ts when seen, else last observed event
--   span_minutes             FLOAT64
--   create_event_ts          TIMESTAMP   -- explicit CreateCluster (NULL = created before window start)
--   delete_event_ts          TIMESTAMP   -- explicit DeleteCluster (NULL = still alive at window end)
--   has_explicit_create      BOOL
--   has_explicit_delete      BOOL
--   total_events             INT64       -- diagnostic across all incarnations of this cluster_name
--
-- Row taxonomy:
--   * Both flags TRUE  -> short-lived cluster fully observed in window.
--   * Create only      -> cluster still alive at window end (delete_event_ts=NULL).
--   * Delete only      -> cluster created before window start (create_event_ts=NULL).
--   * Both flags FALSE -> long-lived cluster spanning the whole 24h window.
--
-- Notes:
--   * proto_payload is a STRUCT in this Log Analytics view, so we access
--     proto_payload.audit_log.method_name directly (no JSON_VALUE).
--   * Pairing rule: for each CreateCluster ts c on a given cluster_name, pair
--     with the smallest DeleteCluster ts d such that d > c. Deletes with no
--     preceding Create in the window are emitted as orphan rows (older
--     incarnation, created before window start).

WITH base AS (
  SELECT
    timestamp,
    -- resource.labels.* is JSON-typed; JSON_VALUE returns a true STRING.
    JSON_VALUE(resource.labels.cluster_name) AS cluster_name,
    log_name,
    proto_payload
  FROM `db-prd-rn63-pwcclake-es.global._Default._Default`
  WHERE resource.type = 'cloud_dataproc_cluster'
    AND JSON_VALUE(resource.labels.cluster_name) IS NOT NULL
),
audit AS (
  -- Admin-activity audit log: Dataproc CreateCluster / DeleteCluster operations.
  SELECT
    timestamp,
    cluster_name,
    proto_payload.audit_log.method_name AS method_name
  FROM base
  WHERE log_name LIKE '%cloudaudit.googleapis.com%2Factivity'
    AND proto_payload.audit_log.method_name IS NOT NULL
),
creates_raw AS (
  SELECT cluster_name, timestamp AS create_ts
  FROM audit
  WHERE method_name LIKE '%.CreateCluster'
),
deletes_raw AS (
  SELECT cluster_name, timestamp AS delete_ts
  FROM audit
  WHERE method_name LIKE '%.DeleteCluster'
),
-- Audit logs emit multiple entries per long-running operation (request + completion,
-- separated by cluster-provisioning time). Collapse consecutive same-type events
-- (with no opposite-type event between them) into ONE canonical event, taking the
-- earliest timestamp = when the API call was made = when billing started/stopped.
creates AS (
  SELECT cluster_name, MIN(create_ts) AS create_ts
  FROM (
    SELECT
      c.cluster_name,
      c.create_ts,
      (SELECT MAX(d.delete_ts) FROM deletes_raw d
        WHERE d.cluster_name = c.cluster_name
          AND d.delete_ts    < c.create_ts) AS prev_delete_ts
    FROM creates_raw c
  )
  GROUP BY cluster_name, prev_delete_ts
),
deletes AS (
  SELECT cluster_name, MIN(delete_ts) AS delete_ts
  FROM (
    SELECT
      d.cluster_name,
      d.delete_ts,
      (SELECT MAX(c.create_ts) FROM creates_raw c
        WHERE c.cluster_name = d.cluster_name
          AND c.create_ts    < d.delete_ts) AS prev_create_ts
    FROM deletes_raw d
  )
  GROUP BY cluster_name, prev_create_ts
),
-- Pair each canonical Create with the soonest canonical Delete after it.
paired AS (
  SELECT
    c.cluster_name,
    c.create_ts,
    (SELECT MIN(d.delete_ts)
       FROM deletes d
      WHERE d.cluster_name = c.cluster_name
        AND d.delete_ts > c.create_ts) AS delete_ts
  FROM creates c
),
-- Orphan deletes: a Delete with no preceding Create in the window
-- (the cluster was created before window start).
orphan_deletes AS (
  SELECT
    d.cluster_name,
    CAST(NULL AS TIMESTAMP) AS create_ts,
    d.delete_ts
  FROM deletes d
  WHERE NOT EXISTS (
    SELECT 1 FROM creates c
    WHERE c.cluster_name = d.cluster_name
      AND c.create_ts    < d.delete_ts
  )
),
incarnations AS (
  SELECT * FROM paired
  UNION ALL
  SELECT * FROM orphan_deletes
),
fallback AS (
  -- First/last event timestamps across all logs for this cluster_name (any incarnation).
  -- Used to fill missing endpoints; total_events is purely diagnostic.
  SELECT
    cluster_name,
    MIN(timestamp) AS first_event_ts,
    MAX(timestamp) AS last_event_ts,
    COUNT(*)       AS total_events
  FROM base
  GROUP BY cluster_name
),
explicit_rows AS (
  SELECT
    i.cluster_name,
    i.create_ts AS create_event_ts,
    i.delete_ts AS delete_event_ts,
    -- Order incarnations by whatever endpoint we know about.
    ROW_NUMBER() OVER (
      PARTITION BY i.cluster_name
      ORDER BY COALESCE(i.create_ts, i.delete_ts)
    ) AS incarnation_idx,
    f.first_event_ts,
    f.last_event_ts,
    f.total_events
  FROM incarnations i
  LEFT JOIN fallback f USING (cluster_name)
),
-- Clusters that appear in `base` but have neither Create nor Delete in the
-- window (long-lived clusters that span the entire 24h retention).
windowed_only AS (
  SELECT
    f.cluster_name,
    CAST(NULL AS TIMESTAMP) AS create_event_ts,
    CAST(NULL AS TIMESTAMP) AS delete_event_ts,
    1                       AS incarnation_idx,
    f.first_event_ts,
    f.last_event_ts,
    f.total_events
  FROM fallback f
  WHERE NOT EXISTS (
    SELECT 1 FROM incarnations i WHERE i.cluster_name = f.cluster_name
  )
),
all_rows AS (
  SELECT * FROM explicit_rows
  UNION ALL
  SELECT * FROM windowed_only
)
SELECT
  cluster_name,
  incarnation_idx,
  COALESCE(create_event_ts, first_event_ts) AS span_start_ts,
  COALESCE(delete_event_ts, last_event_ts)  AS span_end_ts,
  TIMESTAMP_DIFF(
    COALESCE(delete_event_ts, last_event_ts),
    COALESCE(create_event_ts, first_event_ts),
    SECOND
  ) / 60.0                                    AS span_minutes,
  create_event_ts,
  delete_event_ts,
  (create_event_ts IS NOT NULL) AS has_explicit_create,
  (delete_event_ts IS NOT NULL) AS has_explicit_delete,
  total_events
FROM all_rows
ORDER BY cluster_name, incarnation_idx;
