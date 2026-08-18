<!-- gh issue create
  --title "C2: Specialised agents (Tuner Proposal, L3 Optimiser, L2 Failure Analyst)"
  --label "roadmap"
  --label "agentic"
  --label "help-wanted"
-->

# C2 — Specialised agents

## Motivation

The tuner today emits structured JSON + CSV recommendations and a dashboard for humans to interpret. C2 wraps three agent personas around that output — agents that read the recommendations, the underlying metrics, and (for one of them) failed PRD job logs, and surface actionable insights without a human staring at the dashboard.

## Sub-tasks (one per agent)

- **Agent 1 — Tuner Proposal**: reads the existing `_*.json` outputs + bNN.csv inputs at recipe + cluster level, applies trend logic (covariances, z-scores, Pearson) already in the codebase, and emits a structured tuning-recommendation report (Markdown or Slack-formatted). Closes the loop "tuner ran → here's what I'd merge."
- **Agent 2 — L3 Spark Job Optimiser**: deep job-level optimisation suggestions (shuffle, caching, parallelism, broadcast hints) using `ExecutorTrackingListener` evolution + Spark internal APIs. Inspired by Databricks Optimiser-style tooling.
- **Agent 3 — L2 PRD Failure Analyst**: analyses failed PRD jobs (`BQ.EXECUTION_TABLES` → logs → root cause → action). Talks to Agents 1 + 2 for context (e.g. "performance degradation upstream → OOM crash chain").

## Acceptance criteria

- Each agent has a clear input contract (which files / GCP signals it consumes) and output contract (what report it emits, in what format).
- Agent-to-agent (A2A) communication for cross-agent flows (Agent 3 escalating to Agent 1).
- Security guardrails: least-privilege Service Accounts, scoped BQ + GCS read access, no exfiltration paths beyond the configured output channel.

## Open questions

- ADK on GCP — Vertex AI Agent Builder, custom Cloud Run microservices, or something else?
- A2A protocol — proprietary JSON, MCP, ACP, A2A standard? Settle by Agent 3 design.
- LLM choice — Gemini, Claude, mix? Evaluate per-agent given different reasoning needs.

## References

- ROADMAP.md — initiative C2
- _AUTO_TUNING.md — TrendDetector, StatisticalAnalysis (Agents 1 + 3 lean on these)
- _PARALLELISM.md — ExecutorTrackingListener (Agent 2's primary signal)
