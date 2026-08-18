<!-- gh issue create
  --title "Maven Central publishing pipeline (Sonatype + GPG)"
  --label "enhancement"
  --label "release"
-->

# Maven Central publishing pipeline

## Motivation

The project is OSS-shaped (Apache-2.0, semver-tagged) but consumers can't `<dependency>` it. Publishing to Maven Central via Sonatype unlocks library consumption for anyone who wants to embed `ExecutorTrackingListener` or the `RefinementVitamins` framework in their own project.

## Sub-tasks

- Create Sonatype account + claim `com.db.serna` namespace (or pick a public-friendly groupId).
- Configure `pom.xml` `<distributionManagement>` + GPG signing via `maven-gpg-plugin`.
- Add `release.yml` GitHub Actions workflow triggered on git tag.
- Document the release process in CONTRIBUTING.md.

## Acceptance

`git tag v0.1.0 && git push --tags` triggers a workflow that publishes signed artifacts to Maven Central.

## Effort

L (Sonatype account creation can take days; GPG key management is non-trivial)

## Reference

SP-1 spec — explicitly punted from SP-1 scope.
