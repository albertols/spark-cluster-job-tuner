<!-- gh issue create
  --title "C1: Make the tuner deployable in GCP (Cloud Run + Terraform)"
  --label "roadmap"
  --label "gcp"
  --label "help-wanted"
-->

# C1 — Make the tuner deployable in GCP

## Motivation

The tool is local-only today: the user manually exports CSVs from BigQuery, runs `./mvnw`, and opens `./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh`. To grow beyond a single-laptop tool, it needs to land on GCP — fetch BigQuery exports automatically, host the dashboard somewhere addressable, and persist outputs across sessions.

This unlocks scheduled / on-demand tuning runs without anyone in the loop, and makes the dashboard shareable with a team without zip-and-email.

## Sub-tasks

- **BQ export automation**: scheduled (Cloud Scheduler → Cloud Run job) and on-demand (Cloud Run HTTP trigger). Writes CSVs to a GCS bucket with date-partitioned paths.
- **Frontend hosting**: deploy the static dashboard + the small `TunerService` Java backend on Cloud Run (or App Engine — sub-decision). Configure to read inputs and write outputs from the GCS bucket.
- **Cache layer**: GCS bucket for tuner JSON/CSV outputs so the dashboard doesn't re-run the tuner on every page load.
- **Terraform IaC**: `infra/` module covering the bucket, Cloud Run services, IAM bindings, scheduler job, and necessary BigQuery roles.

## Acceptance criteria

- A push to `main` (or a manual button) triggers a fresh tuning run end-to-end on GCP.
- `https://tuner.<your-domain>.run.app` (or similar) shows the latest dashboard.
- Service accounts follow least-privilege; no broad `roles/owner` grants.
- A `terraform apply` from a clean GCP project bootstraps the whole thing in <10 minutes.

## Open questions

- Cloud Run vs App Engine for the frontend? Cloud Run is simpler and cheaper for low-traffic; App Engine has built-in static-file caching.
- ADK on GCP? Or roll our own deployment tooling?
- GCS bucket layout: per-date `gs://bucket/inputs/<YYYY_MM_DD>/` mirroring local layout, or flat with date metadata?

## References

- README §3 "How it works" — the local pipeline this distributed version mirrors
- ROADMAP.md — initiative C1
