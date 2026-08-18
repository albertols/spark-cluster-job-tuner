<!-- gh issue create
  --title "Tighten scalafix DisableSyntax (re-enable noReturns + noWhileLoops)"
  --label "enhancement"
  --label "code-quality"
-->

# Tighten scalafix DisableSyntax

## Motivation

SP-1 landed scalafix with `DisableSyntax{noXml=true}` only. The `noReturns` and `noWhileLoops` rules were intentionally relaxed because the codebase has 86 `return` sites and 24 `while` loops in production code that would all need refactoring (`KeptRecipeCarrier`, `BoostMetadataCarrier`, `ClusterMachineAndRecipeTuner`, etc.).

## Sub-tasks

- Refactor the 110 sites to vals / tail-recursion / iterator patterns.
- Re-enable `noReturns = true` and `noWhileLoops = true` in `.scalafix.conf`.

## Acceptance

`./mvnw -B compile test-compile scalafix:scalafix -Dscalafix.mode=CHECK` passes with both rules enabled.

## Effort

L (large refactor across many files)

## Reference

SP-1 plan, T3 step 3.7 — surfaced during the original scalafix run.
