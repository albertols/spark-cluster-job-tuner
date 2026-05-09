<!-- gh issue create
  --title "Upgrade Maven wrapper to script-only flavour (drop bundled jar)"
  --label "enhancement"
  --label "build"
-->

# Upgrade Maven wrapper to script-only flavour

## Motivation

SP-1 generated `mvnw` via `io.takari:maven:0.7.7:wrapper` (the legacy Takari flavour), which bundles a ~50 KB `.mvn/wrapper/maven-wrapper.jar` into the repo. The modern Apache Maven Wrapper supports a script-only flavour (`mvn wrapper:wrapper -Dtype=only-script`) that downloads the wrapper jar lazily on first invocation, removing the binary from git.

## Sub-tasks

- Run `mvn wrapper:wrapper -Dtype=only-script` from a recent Maven (3.9.6+) to regenerate.
- Verify `./mvnw -v` still works on a fresh clone.
- Delete the now-unused `.mvn/wrapper/maven-wrapper.jar` and `MavenWrapperDownloader.java`.

## Acceptance

Repo no longer ships a wrapper binary; `git clone && ./mvnw -v` still resolves Maven 3.9.6.

## Effort

S (regenerate + delete + verify)

## Reference

SP-1 final whole-branch review.
