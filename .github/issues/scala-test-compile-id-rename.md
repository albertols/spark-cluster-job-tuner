<!-- gh issue create
  --title "Rename serve profile's scala-test-compile execution id"
  --label "bug"
  --label "build"
-->

# Rename serve profile's scala-test-compile execution id

## Motivation

`pom.xml` has `<id>scala-test-compile</id>` in BOTH the top-level `<build>` and the `serve` profile. Maven merges executions by id, so under `-Pserve` the profile's `process-test-resources` phase overrides the top-level's `test-compile` phase. Works today (functionally equivalent) but it's a footgun — a future contributor changing one and not the other will hit confusing behaviour.

## Sub-tasks

- Rename the serve-profile execution to `scala-test-compile-serve` (or similar — non-colliding id).
- Verify `./mvnw -Pserve package` still produces the slim jar.

## Acceptance

Both executions are independently bound; `mvn help:effective-pom -Pserve` shows two distinct executions for the plugin.

## Effort

S (3-line pom.xml change + verification)

## Reference

SP-1 final whole-branch review.
