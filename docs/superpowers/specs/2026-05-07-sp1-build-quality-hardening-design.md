# SP-1 — Build & Quality Hardening (OSS Readiness)

**Status:** Draft for review
**Date:** 2026-05-07
**Owner:** @albertols
**Sub-project of:** OSS Readiness (parent epic — see decomposition below)
**Successor specs:** SP-2 (OSS landing surface), SP-3 (community infra), SP-4 (Medium articles)

## Context

The `spark-cluster-job-tuner` repository is moving from internal use to public OSS publication. Today the build is IntelliJ-driven: Maven has no Scala compiler plugin in its default profile, no test execution plugin, no coverage, no static analysis, and no continuous integration. There are 28 ScalaTest specs in `src/test/scala/` that **`mvn test` does not currently run** — they are only executed inside IntelliJ.

SP-1 is the first sub-project of the OSS readiness epic. Its purpose is to establish the build- and quality-signal foundation that the README badges (SP-2), `CONTRIBUTING.md` (SP-3), and contributor expectations all depend on. Without SP-1, badges would point to nothing, contributor PRs would have no automated validation, and the project would land on GitHub looking abandoned.

## Goals

1. `./mvnw verify` from a fresh clone compiles, runs all 28 ScalaTest specs, and exits non-zero on any test failure.
2. Coverage is measured and reported (Scoverage → Codecov), no PR gating.
3. Code style and lint checks (scalafmt + scalafix) run in CI on every PR.
4. CI is GitHub Actions, single workflow file, three parallel jobs (`lint`, `test`, `coverage`).
5. Repo lands fully OSS-shaped on day one: `LICENSE` (Apache-2.0), `SECURITY.md`, `dependabot.yml`, CodeQL workflow.
6. The existing `serve` profile (used by `frontend/serve.sh --api` and the slim-jar build) is **not perturbed**.

## Non-goals (deferred to follow-up issues, surfaced in SP-3)

- Maven Central publishing pipeline (Sonatype + GPG signing).
- Branch protection / required-status-check configuration (GitHub repo settings, not code).
- Coverage thresholds or no-regression gating.
- New tests beyond what already exists.
- Mutation testing, performance benchmarks, CHANGELOG generation.
- README badge wiring — SP-1 produces the URL shapes; SP-2 owns presentation.

## Decisions (locked in during brainstorming)

| Decision | Choice | Rationale |
|---|---|---|
| License | **Apache-2.0** | Dominant in Spark/Hadoop ecosystem; explicit patent grant; enterprise-friendly. |
| Coverage tool | **Scoverage** | Scala-native, accurate on case classes/pattern matches; Codecov supports its Cobertura XML output. |
| Coverage gating | **Report-only** | No measured baseline yet; ship signal first, upgrade to no-regression gate later (SP-3 issue). |
| Static analysis | **scalafmt + scalafix** | Hits value/friction sweet spot; both auto-fixable in one local command. WartRemover excluded (high friction on legacy code). |
| Java baseline | **Java 11** | Matches current `pom.xml` and Spark 3.5.3 support matrix. |
| Test runner | **scalatest-maven-plugin** | Idiomatic for ScalaTest; avoids Surefire forking quirks. |
| Maven wrapper | **Yes** (`mvnw`/`mvnw.cmd`) | Reproducible build for first-time contributors; standard OSS practice. |
| CI provider | **GitHub Actions** | Only sensible answer for OSS in 2026. |
| Workflow shape | **One `ci.yml`, three parallel jobs** | Faster feedback than monolithic single-job; less YAML drift than multi-file. |
| Dependabot | **Yes** | Weekly Maven + GHA updates, max 5 open PRs. |
| CodeQL | **Yes** | GitHub-native, free for public repos, weekly schedule. |
| `SECURITY.md` | **Yes, in SP-1** | Cheap to bundle; lands repo as fully OSS-shaped on day one. |
| Maven Central publish | **Punted to SP-3 issue** | Non-trivial setup (Sonatype, GPG); not blocking initial publication. |

## Deliverables

### New files

```
LICENSE                                     # Apache-2.0 full text
SECURITY.md                                 # vulnerability disclosure policy
.scalafmt.conf                              # formatter config
.scalafix.conf                              # linter rules
mvnw                                        # POSIX Maven wrapper
mvnw.cmd                                    # Windows Maven wrapper
.mvn/wrapper/maven-wrapper.properties       # wrapper version pin
.github/dependabot.yml                      # auto-PRs for Maven + GHA
.github/workflows/ci.yml                    # lint / test / coverage
.github/workflows/codeql.yml                # weekly security scan
```

### Modified files

- `pom.xml` — add 5 plugins to top-level `<build>`. **No dependency changes.** The `serve` profile is untouched.
- `.gitignore` — add `.bsp/`, `.metals/`, `.vscode/`, `target/site/scoverage*`.

## Architecture

### `pom.xml` plugin set

All plugins go in the **top-level `<build><plugins>`** so `mvn verify` exercises them by default. The existing `serve` profile retains its own scala-maven-plugin (no conflict — profile plugins are additive).

| Plugin | Coordinates | Version | Bound phase | Notes |
|---|---|---|---|---|
| Scala compiler | `net.alchim31.maven:scala-maven-plugin` | 4.9.2 | `compile`, `test-compile` | Adds `src/main/scala` and `src/test/scala` to compile sources. |
| Surefire skip | `org.apache.maven.plugins:maven-surefire-plugin` | 3.5.0 | `test` | Configured with `<skipTests>true</skipTests>` so it does not try to run JUnit on Scala specs. |
| ScalaTest runner | `org.scalatest:scalatest-maven-plugin` | 2.2.0 | `test` | Default goal `test`, runs `*Spec.scala`. |
| Scoverage | `org.scoverage:scoverage-maven-plugin` | 2.0.4 | `verify` (under `coverage` profile only) | Goal `report` produces Cobertura XML at `target/site/scoverage.xml`. |
| Scalafmt | `org.antipathy:mvn-scalafmt_2.12` | 1.1.1640084764.9f463a9 | `validate` | `mvn scalafmt:format` to fix; `mvn scalafmt:check` enforces in CI. |
| Scalafix | `io.github.evis:scalafix-maven-plugin_2.12` | 0.1.10_0.1.4 | manual + CI | `mvn scalafix:scalafix` to fix; `-Dscalafix.mode=CHECK` enforces in CI. |

> All versions are starting points sourced from the latest stable releases as of 2026-05-07. The implementation plan (SP-1's writing-plans output) will pin them and verify they resolve cleanly against Maven Central.

### Coverage profile

```xml
<profile>
  <id>coverage</id>
  <build>
    <plugins>
      <plugin>
        <groupId>org.scoverage</groupId>
        <artifactId>scoverage-maven-plugin</artifactId>
        <version>2.0.4</version>
        <configuration>
          <scalaVersion>${scala.version.major}.${scala.version.minor}</scalaVersion>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>report</goal>
            </goals>
            <phase>verify</phase>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</profile>
```

Daily `./mvnw verify` skips coverage instrumentation (~3 min). `./mvnw -Pcoverage verify` runs the instrumented build (~5–6 min) and writes `target/site/scoverage.xml`.

### Scalafmt config (`.scalafmt.conf`)

```hocon
version = "3.8.3"
runner.dialect = scala212
maxColumn = 120
align.preset = none
docstrings.style = Asterisk
trailingCommas = preserve
project.git = true
```

Conservative on purpose: no aggressive rewrites that would explode the diff or contradict existing house style.

### Scalafix config (`.scalafix.conf`)

```hocon
rules = [
  RemoveUnused,
  LeakingImplicitClassVal,
  ProcedureSyntax,
  NoAutoTupling,
  DisableSyntax
]

DisableSyntax {
  noVars = false        # repo intentionally uses vars in TestSparkSession mixins
  noNulls = false       # null is unavoidable in Spark interop
  noReturns = true
  noWhileLoops = true
  noXml = true
}
```

Style-opinion rules (`OrganizeImports`, `RedundantSyntax`) intentionally excluded for SP-1; can be added in a SP-3 follow-up.

### CI workflow — `.github/workflows/ci.yml`

Single file, three parallel jobs, all `ubuntu-latest`, all share a Maven cache keyed on `hashFiles('**/pom.xml')`.

```yaml
name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: 'temurin', java-version: '11', cache: 'maven' }
      - run: ./mvnw -B scalafmt:check
      - run: ./mvnw -B scalafix:scalafix -Dscalafix.mode=CHECK

  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: 'temurin', java-version: '11', cache: 'maven' }
      - run: ./mvnw -B test

  coverage:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: 'temurin', java-version: '11', cache: 'maven' }
      - run: ./mvnw -B -Pcoverage verify scoverage:report
      - uses: codecov/codecov-action@v4
        with:
          files: ./target/site/scoverage.xml
          fail_ci_if_error: false
```

**Trigger choice:** `push` to `main` + `pull_request` to `main`. No schedule. No matrix.

**Codecov auth:** tokenless. Public repos can upload to Codecov without a token; we'll add `CODECOV_TOKEN` only if the tokenless flow proves flaky.

### CI workflow — `.github/workflows/codeql.yml`

Standard GitHub-template Java workflow, scheduled weekly, also runs on push to `main`. Generated by GitHub's CodeQL setup wizard; will land verbatim from the official template, only customised to use Java 11.

### Dependabot — `.github/dependabot.yml`

```yaml
version: 2
updates:
  - package-ecosystem: maven
    directory: "/"
    schedule:
      interval: weekly
    open-pull-requests-limit: 5
    labels: ["dependencies", "maven"]

  - package-ecosystem: github-actions
    directory: "/"
    schedule:
      interval: weekly
    open-pull-requests-limit: 5
    labels: ["dependencies", "github-actions"]
```

### `SECURITY.md`

Minimal, GitHub-recognized format:

```markdown
# Security Policy

## Reporting a Vulnerability

If you discover a security vulnerability in `spark-cluster-job-tuner`, please
report it privately by emailing serna.alberto.eng@gmail.com with the subject
"[SECURITY] spark-cluster-job-tuner".

Please do **not** open a public GitHub issue for security reports.

You can expect an acknowledgement within 7 days. Fixes for confirmed
vulnerabilities will be coordinated with you before public disclosure.

## Supported Versions

Only the latest commit on `main` is supported. There are no published releases
yet; once Maven Central publication lands, this section will be updated.
```

### `LICENSE`

Verbatim Apache-2.0 text from https://www.apache.org/licenses/LICENSE-2.0.txt, with the standard copyright preamble:

```
Copyright 2026 Alberto Serna

Licensed under the Apache License, Version 2.0 (the "License");
...
```

## Rollout sequence

Six small commits, each independently reviewable. **Order matters** — getting it wrong leaves `main` red mid-flight.

1. **Format-and-stamp.** Add `.scalafmt.conf` + scalafmt-maven-plugin. Run `mvn scalafmt:format` once. Commit the (potentially large) whitespace-only diff. Open branches will need to rebase — call this out in the PR description.
2. **Lint-and-stamp.** Add `.scalafix.conf` + scalafix-maven-plugin. Run `mvn scalafix:scalafix`. Commit auto-fixes. Hand-fix any remaining issues in the same commit.
3. **Test plugins.** Add scala-maven-plugin (top-level), Surefire skip config, scalatest-maven-plugin, scoverage-maven-plugin (under `coverage` profile). Verify `mvn verify` runs all 28 specs locally. Verify `mvn -Pcoverage verify scoverage:report` writes the XML.
4. **Maven wrapper.** Add `mvnw`, `mvnw.cmd`, `.mvn/wrapper/maven-wrapper.properties`.
5. **CI workflows.** Add `.github/workflows/ci.yml` and `.github/workflows/codeql.yml`. Open as draft PR. Iterate until all checks green.
6. **OSS-shape commit.** Add `LICENSE`, `SECURITY.md`, `.github/dependabot.yml`. Update `.gitignore`.

Each commit can be a separate PR if preferred, or all six can ship in one branch with a clean linear history.

## Verification gates

| Gate | Command | Expected |
|---|---|---|
| Tests run via Maven | `./mvnw -B verify` | All 28 ScalaTest specs execute; build green. |
| Coverage report | `./mvnw -B -Pcoverage verify scoverage:report` | `target/site/scoverage.xml` exists with non-zero coverage. |
| Format check | `./mvnw -B scalafmt:check` | Exit 0. |
| Lint check | `./mvnw -B scalafix:scalafix -Dscalafix.mode=CHECK` | Exit 0. |
| `serve` profile intact | `./mvnw -Pserve package` | Slim jar produced; `frontend/serve.sh --api` boots. |
| CI on PR | Draft PR | `lint` / `test` / `coverage` / `CodeQL` all green. |
| Codecov badge | First merged PR | Badge URL renders a number, not "unknown" (allow ~10 min). |

## Risks

- **Whitespace bomb (commit 1):** scalafmt-format on ~4k existing LOC will produce a large noisy diff. Mitigation: it is the **first** commit, sets a clean baseline; document the rebase requirement.
- **`scala-maven-plugin` duplication with the `serve` profile:** the existing `serve` profile declares `scala-maven-plugin` with a deliberate `process-resources` phase override (per the comment in `pom.xml`, this was a workaround so the slim-jar contains compiled classes). When SP-1 adds the plugin at the top level (default `compile` phase), `mvn -Pserve package` will activate **both** declarations. The implementation plan must decide one of: (a) keep both — Maven merges plugin config and the additional `process-resources` binding still fires (preferred, smallest diff to the `serve` flow); (b) remove the profile's `scala-maven-plugin` block entirely if the top-level binding runs early enough to populate classes before the slim-jar packages — this needs verification with `mvn -Pserve package` and inspecting the resulting jar contents. Do **not** silently delete the profile's plugin without re-verifying the `frontend/serve.sh --api` boot path.
- **Spark-session port collisions in CI:** existing test mixins clear `spark.driver.port` between suites; no new risk.
- **Plugin version drift:** versions pinned at 2026-05-07. The implementation plan must verify each resolves against Maven Central before committing.
- **Codecov first-upload latency:** badge will say "unknown" for ~10 min after the first successful upload. Cosmetic, not a failure.
- **Dependabot noise:** weekly + max 5 open PRs is conservative; tunable in a follow-up if it becomes annoying.

## Dependencies on other sub-projects

- **SP-2 (landing surface):** consumes the badge URLs produced by SP-1 (CI status, Codecov, license, Scala/Spark versions). SP-1 must land *before* SP-2's README pass.
- **SP-3 (community infra):** picks up the deferred items (Maven Central publishing, branch protection, coverage gating, Spotless/SECURITY.md hardening).
- **SP-4 (Medium articles):** unaffected by SP-1.

## Open questions

None at spec time. Any version-resolution failures during implementation become tactical decisions inside the writing-plans output.
