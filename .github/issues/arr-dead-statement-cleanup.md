<!-- gh issue create
  --title "Drop orphaned arr(...) expression on GenerationSummary.scala:146"
  --label "cleanup"
  --label "good first issue"
-->

# Drop orphaned arr(...) expression

## Motivation

In SP-1, scalafix's `RemoveUnused` removed `val quotaArray: String = ` from a line that previously read `val quotaArray: String = arr(...) // unused`, leaving `arr(...) // unused` as a pure expression-as-statement with no effect — orphaned dead code.

## Sub-tasks

- Open `src/main/scala/com/db/serna/orchestration/cluster_tuning/single/GenerationSummary.scala` around line 146.
- Delete the orphaned `arr(...)` line and the `// unused` comment.
- Verify `./mvnw -B verify` still passes (the line was already a no-op, so this should be a clean removal).

## Acceptance

Line is gone; tests pass; nothing else changes.

## Effort

XS (one line delete)

## Reference

SP-1 T3 quality review.
