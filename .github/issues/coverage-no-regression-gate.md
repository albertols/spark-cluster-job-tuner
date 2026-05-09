<!-- gh issue create
  --title "Coverage no-regression gate on PRs"
  --label "enhancement"
  --label "ci"
-->

# Coverage no-regression gate on PRs

## Motivation

SP-1's coverage CI job is report-only — it uploads to Codecov but doesn't fail PRs that drop coverage. A no-regression gate (fail PR if coverage drops more than e.g. 1% vs main) prevents bit-rot without setting an arbitrary floor.

## Sub-tasks

- Configure Codecov's `codecov.yml` with a `target: auto` patch threshold + main branch target.
- Or: add a `coverage-gate` step to `ci.yml` that compares against base branch.

## Acceptance

A PR that drops branch coverage by >1% gets a failing CI check.

## Effort

M (the policy is the hard part — too strict blocks legitimate refactors; too loose is theatre)

## Reference

SP-1 spec Q3 — coverage gating was deferred to "report-only" pending baseline.
