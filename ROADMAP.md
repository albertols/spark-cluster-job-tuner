# Roadmap

## Active sub-projects (OSS readiness epic)

- ✅ **SP-1 — Build & Quality Hardening** — merged via [PR #1](https://github.com/albertols/spark-cluster-job-tuner/pull/1). Maven wrapper, ScalaTest+Scoverage, Spotless+scalafix, GitHub Actions CI+CodeQL, LICENSE, SECURITY.md, Dependabot.
- 🟡 **SP-2 — OSS Landing Surface** — this PR. World-class README, frontend index.html landing, 3 Mermaid diagrams, 5 marquee-feature screenshots, structured SQL header comments, this `ROADMAP.md`.
- ⬜ **SP-3 — Community Infra** — `CONTRIBUTING.md`, issue templates, PR templates, the kickoff GitHub Issues that THIS roadmap links to via `[#TBD]`.
- ⬜ **SP-4 — Medium articles** — long-form posts on the design (PART_1 telemetry, PART_2 tuner internals, PART_4 future direction).

## Major future initiatives (proposed; SP-3 will fill the linked Issues)

### C1 — GCP-deployable [#TBD]

Move the tool from local-only to a GCP-hosted deployment.

- Fetch `b*.csv` exports automatically from BigQuery (schedule + on-demand) instead of manual export.
- Deploy the frontend + a small backend on Cloud Run / App Engine / similar.
- Cache CSV / JSON outputs in GCS for performance.
- Manage all of the above with Terraform (IaC).

### C2 — Specialised agents [#TBD]

Three agent personas, eventually composed via an A2A (Agent-to-Agent) protocol:

- **Agent 1 — Tuner Proposal**: pure metrics + groomed trends (covariances, correlations, z-score) at recipe + cluster level. Emits structured tuning-recommendation reports.
- **Agent 2 — L3 Spark Job Optimiser**: deep job-level optimisation (shuffle, caching, parallelism) using [`ExecutorTrackingListener`](/src/main/scala/com/db/serna/utils/spark/parallelism/ExecutorTrackingListener.scala) evolution and Spark internal APIs. Inspired by Databricks Optimiser-style tooling.
- **Agent 3 — L2 PRD Failure Analyst**: analyses failed PRD jobs (`BQ.EXECUTION_TABLES` → logs → actions). Talks to Agents 1 + 2 (e.g., performance degradation → OOM crash chain).

Plus: ADK on GCP, security guardrails (least-privilege Service Accounts), wrappers for BigQuery / GCS access.

### C3 — Markov chains for predictions [#TBD]

State-transition prediction of future cluster/job state from current + historical data. Scenario simulation: assess the impact of different tuning strategies on performance + cost. Matrix of states from observed log-analytics signals.

## Smaller follow-ups (already tracked from SP-1's plan doc)

- Tighten `DisableSyntax` rules — re-enable `noReturns` + `noWhileLoops` after refactoring 86 returns + 24 while-loops in production code [#TBD]
- Rename `serve` profile's `scala-test-compile` execution id to avoid Maven's merge-by-id override [#TBD]
- Maven wrapper script-only flavour (drop the bundled 50KB jar) [#TBD]
- `.gitattributes` for `mvnw` / `mvnw.cmd` line endings (Windows protection) [#TBD]
- Maven Central publishing pipeline (Sonatype + GPG) [#TBD]
- Coverage no-regression gate on PRs (currently report-only) [#TBD]
- Branch protection rules for `main` (currently relies on review discipline) [#TBD]
- Logo / brand identity [#TBD]
- Drop the orphaned `arr(...)` expression on [`GenerationSummary.scala:146`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/single/GenerationSummary.scala#L146) [#TBD]
- `serve.sh` Windows portability (cp + bash dependencies) [#TBD]
- `index.html` landing: code-block syntax highlighting (highlight.js or prism.js) [#TBD]
