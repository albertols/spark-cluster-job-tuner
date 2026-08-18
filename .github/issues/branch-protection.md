<!-- gh issue create
  --title "Branch protection rules for main"
  --label "enhancement"
  --label "repo-config"
-->

# Branch protection rules for main

## Motivation

Today merging to `main` relies on review discipline. Branch protection makes the rules enforceable: require all CI checks green, require at least 1 approving review (when a team forms; until then, allow self-merge), block force-pushes, block deletions.

The Dependabot scoverage 2.1.5 incident — which broke main's coverage CI — would have been caught by a status-check requirement.

## Sub-tasks

- Settings → Branches → Add rule for `main`:
  - Require status checks: `lint`, `test`, `coverage`, `Analyze (Java/Scala)`
  - Require linear history (squash merges only)
  - Block force-push + deletion
- Document in CONTRIBUTING.md (PR process section).

## Acceptance

Pushing directly to `main` or merging a PR with red CI is rejected by GitHub.

## Effort

S (one-time settings change; no code)

## Reference

SP-3 spec § Risks; SP-1 follow-up backlog.
