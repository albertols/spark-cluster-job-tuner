# SP-3 — Community Infra (CONTRIBUTING + Templates + Kickoff Issues)

**Status:** Draft for review
**Date:** 2026-05-08
**Owner:** @albertols
**Sub-project of:** OSS Readiness epic (parent decomposition: SP-1 ✅ done & merged via PR #1; SP-2 ✅ done & merged via PR #11; SP-3 this; SP-4 Medium articles).

## Context

After SP-1 (build + quality) and SP-2 (landing surface) landed, the repo presents to the world but lacks the contributor-onboarding scaffolding that turns visitors into PR authors. SP-3 fills that gap:

1. A comprehensive `CONTRIBUTING.md` with architecture orientation + "How to add a `TuningStrategy` / `RefinementVitamin` / `bNN` query / dashboard tab" playbooks.
2. GitHub-recognised community-health files: `CODE_OF_CONDUCT.md`, `.github/PULL_REQUEST_TEMPLATE.md`, three `.github/ISSUE_TEMPLATE/*.yml` files plus a `config.yml` chooser.
3. **14 GitHub Issues** that fill the `[#TBD]` placeholders SP-2 left in `ROADMAP.md` — drafted as markdown stubs in `.github/issues/`, version-controlled in this PR for review, then opened via `gh issue create --body-file` after merge.

The 14 Issues split into 3 big initiatives (C1 GCP-deployable, C2 specialised agents, C3 Markov chains) at ~250 words each, plus 11 smaller follow-ups (the SP-1 + SP-2 backlogs) at ~80 words each.

After SP-3 merges, the OSS-readiness epic has only **SP-4 (Medium articles)** remaining.

## Goals

1. New CONTRIBUTING.md gives a first-time contributor enough orientation to open a real PR within 30 minutes — Quickstart for the impatient, Architecture orientation for the deep readers, and 4 step-by-step playbooks for the most common contributions.
2. New community-health surfaces (CoC, PR template, Issue templates) tick GitHub's community-health checklist boxes.
3. The 14 issue body markdown files are reviewable in PR before they become Issues — same rigor we'd apply to a spec.
4. Q&A traffic redirects to GitHub Discussions (`blank_issues_enabled: false` + `config.yml` chooser) instead of cluttering the Issue tracker.
5. README §10 Contributing replaces SP-2's placeholder mini-guidance with real CONTRIBUTING + CoC + Discussions links.
6. Zero perturbation to existing code, build, tests, dashboard. Pure docs + repo metadata.

## Non-goals (deferred)

- `CODEOWNERS` file (per Q4 — solo project + branch protection is the real defence).
- Branch protection rules on `main` (becomes one of SP-3's own kickoff Issues — manual settings change, not a file commit).
- Auto-labelling rules / GitHub Actions for issue/PR triage (premature).
- Cosmetic micro-fixes from SP-2 reviews (focus-style on landing.css, footer width, mermaid `direction TB` redundancy, app.js:4153 comment) — too small to track as Issues; will get addressed organically.
- Logo design itself (becomes one of SP-3's own kickoff Issues).
- The actual implementation of any of the 14 Issues — SP-3 only OPENS them; future PRs close them.

## Decisions (locked in during brainstorming)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| Q1 | Decomposition shape | **Combined sub-project: in-repo files + drafted issue bodies + opened via `gh` after merge** | Audit-trailed issue bodies are spec-equivalent design rigor; deserve PR review, not ad-hoc GitHub UI. |
| Q2 | CONTRIBUTING.md scope | **Comprehensive (~250 lines)** with architecture orientation + 4 "How to add..." playbooks | SP-3 is THE community-infra sub-project; minimal would undercut OSS-readiness mission. |
| Q3 | Issue templates + Q&A redirect | **3 templates (Bug + Feature + Documentation); Q&A → GitHub Discussions** | Bug and Feature obvious; Documentation earns its slot because the project ships deep `_*.md` docs. Discussions is the modern OSS pattern for Q&A. |
| Q4 | PR template + CODEOWNERS | **Minimal PR template + no CODEOWNERS** | 4 sections (what + why + tested + linked-issue) capture all reviewers need. Solo-project CODEOWNERS is theatre; branch protection is the real defence (already a SP-3 follow-up). |
| Q5 | Issue body density | **Big initiatives ~250 words / smaller follow-ups ~80 words** | 250 words signals seriousness without over-prescribing; 80 words is enough for a contributor to claim and start. |
| Q6 | Code of Conduct | **Contributor Covenant 2.1 verbatim** with `alberto.lopez@email.com` | De facto OSS standard in 2026; one less decision; recognised by GitHub. |

## Deliverables

### New files (in-repo, committed via SP-3 PR)

```
CONTRIBUTING.md                            ~250 lines
CODE_OF_CONDUCT.md                          Contributor Covenant 2.1 (~140 lines)
.github/
├── PULL_REQUEST_TEMPLATE.md                ~15 lines
├── ISSUE_TEMPLATE/
│   ├── bug_report.yml                      GitHub Forms YAML
│   ├── feature_request.yml                 GitHub Forms YAML
│   ├── documentation.yml                   GitHub Forms YAML
│   └── config.yml                          chooser config; redirects Q&A to Discussions
└── issues/                                 14 markdown stubs, opened via `gh` after merge
    ├── c1-gcp-deployable.md                ~250 words
    ├── c2-specialised-agents.md            ~250 words
    ├── c3-markov-prediction.md             ~250 words
    ├── tighten-disable-syntax.md           ~80 words
    ├── scala-test-compile-id-rename.md     ~80 words
    ├── mvnw-script-only.md                 ~80 words
    ├── gitattributes-line-endings.md       ~80 words
    ├── maven-central-publish.md            ~80 words
    ├── coverage-no-regression-gate.md      ~80 words
    ├── branch-protection.md                ~80 words
    ├── logo-design.md                      ~80 words
    ├── arr-dead-statement-cleanup.md       ~80 words
    ├── serve-sh-windows.md                 ~80 words
    └── landing-code-syntax-highlighting.md ~80 words
```

### Modified files

- `README.md` — §10 Contributing: replace SP-2 placeholder text with real links (CONTRIBUTING.md + CODE_OF_CONDUCT.md + Discussions).
- `ROADMAP.md` — *follow-up commit AFTER Issues are opened*: replace each `[#TBD]` with the actual issue number (`[#42]` style). NOT part of the SP-3 PR itself; happens in a separate small commit after the manual `gh issue create` session.

### Out of scope

- All items from the "Non-goals" section above.
- The 14 issue *implementations* — SP-3 only OPENS them.

## CONTRIBUTING.md architecture (locked outline)

| § | Section | Words | Content |
|---|---|---|---|
| 1 | Welcome + how to read this doc | 50 | Two paths: 5-min Quickstart (first PR) or skip to Architecture Orientation (depth). |
| 2 | Quickstart (5-minute first PR) | 100 | clone → `./mvnw verify` → tiny change → `./mvnw spotless:apply` → PR. |
| 3 | Architecture orientation | 250 | 5-min map of the codebase using existing `_DESIGN.md` / `_AUTO_TUNING.md` / `_REFINEMENT.md` / `_LOG_ANALYTICS.md` as anchors. **Navigational layer ON TOP, not duplication.** Tells readers what to read in what order. |
| 4 | How to add a `TuningStrategy` | 150 | Step-by-step. Where the trait lives, what to override (`name`, `executorTopology`, `biasMode`, `quotas`), how it surfaces in `wizard.js`, what tests to add. |
| 5 | How to add a `RefinementVitamin` | 200 | Reading a `bNN.csv`, emitting per-recipe boost annotations, lifecycle code (New / Holding / ReBoost), CSS chip colour, tests. |
| 6 | How to add a Log Analytics query (`bNN`) | 150 | When to add, structured header template (per SP-2 T7), Scala loader pattern, tests, README + ROADMAP touchpoints. |
| 7 | How to add a dashboard tab | 100 | Where `wizard.js` + `app.js` + `style.css` interact; how existing tabs (Fleet Overview / Correlations / Divergences) are structured; manual testing via sample data. |
| 8 | Test conventions | 100 | `SparkTestSession` mixin, `TestSparkSessionSupport`, why `local[1]/[2]`, port-collision avoidance, where to put tests. |
| 9 | Code style + lint | 80 | Spotless (auto-formats Scala), scalafix (auto-fixes some lints, fails CI on others). One-liner: `./mvnw spotless:apply scalafix:scalafix`. |
| 10 | PR process | 80 | One PR per logical change, link an issue, ensure CI green (lint/test/coverage/CodeQL). Squash merges by default. |
| 11 | Where to ask questions | 30 | GitHub Discussions for general; SECURITY.md for vulns; `[Question]` issue prefix as fallback. |
| 12 | Code of Conduct | 20 | Link to `CODE_OF_CONDUCT.md`; participation = agreement. |

Total: ~1310 words → ~200-260 lines after Markdown formatting.

**Anti-duplication rule:** §3 ("Architecture orientation") is a NAVIGATION layer pointing readers at `_DESIGN.md` / `_AUTO_TUNING.md` / `_REFINEMENT.md` / `_LOG_ANALYTICS.md` in the right order. It does NOT re-explain content covered there. Same for the §4-§7 playbooks: workflow steps + the bare minimum API surface, deep details linked out.

## Issue templates architecture

### `config.yml` (chooser)

```yaml
blank_issues_enabled: false
contact_links:
  - name: 💬 Question or discussion
    url: https://github.com/albertols/spark-cluster-job-tuner/discussions
    about: For questions, ideas, or general discussion. Issues are for bugs and concrete tasks.
  - name: 🔒 Security vulnerability
    url: https://github.com/albertols/spark-cluster-job-tuner/blob/main/SECURITY.md
    about: Please follow the private disclosure path in SECURITY.md.
```

`blank_issues_enabled: false` forces a template choice — keeps the Issue tracker focused.

### `bug_report.yml`

GitHub-Forms YAML. Title prefix `[Bug] `. Fields:

- Summary (textarea, required)
- Reproduction steps (textarea — numbered list, required)
- Expected behaviour (textarea, required)
- Actual behaviour (textarea, required)
- Environment (textarea — OS / Java / Maven / branch / commit, required)
- Logs / output (collapsible details, optional)

Auto-applied labels: `bug, triage`.

### `feature_request.yml`

Title prefix `[Feature] `. Fields:

- Problem statement (textarea — what pain motivates this, required)
- Proposed solution (textarea, required)
- Alternatives considered (textarea, optional)
- Additional context (textarea, optional)

Auto-applied labels: `enhancement, triage`.

### `documentation.yml`

Title prefix `[Docs] `. Fields:

- What documentation (textarea — file path / section / sentence, required)
- What's wrong or missing (textarea, required)
- Suggested fix or improvement (textarea, optional)

Auto-applied labels: `documentation, triage`.

## PR template (`.github/PULL_REQUEST_TEMPLATE.md`)

```markdown
## Summary

<2-4 lines on what changed and why.>

## Test plan

- [ ] <verification steps>

## Linked issue

Closes #<issue> *(or "n/a — small/follow-up")*
```

## Code of Conduct

Verbatim Contributor Covenant 2.1 fetched from `https://www.contributor-covenant.org/version/2/1/code_of_conduct/code_of_conduct.md`. Substitute `[INSERT CONTACT METHOD]` with `alberto.lopez@email.com`. ~140 lines.

## Issue body inventory (14 markdown files in `.github/issues/`)

Each file has YAML-style frontmatter at the top with `--label` / `--title` hints in HTML comments so that `gh issue create --title "..." --body-file <file> --label "..."` invocations are reproducible from the file alone.

Pattern at top of each file:
```markdown
<!-- gh issue create
  --title "C1: Make the tuner deployable in GCP (Cloud Run + Terraform)"
  --label "roadmap"
  --label "gcp"
  --label "help-wanted"
-->

# C1 — GCP-deployable

## Motivation
…
```

### Big initiatives — ~250 words each

**`c1-gcp-deployable.md`** — Title: `C1: Make the tuner deployable in GCP (Cloud Run + Terraform)`. Labels: `roadmap, gcp, help-wanted`. Sections: Motivation (local-only today; vision: scheduled BQ exports → Cloud Run frontend → GCS-backed cache → Terraform) / Sub-tasks (4-6 bullets) / Acceptance criteria / Open questions / Links to relevant existing docs.

**`c2-specialised-agents.md`** — Title: `C2: Specialised agents (Tuner Proposal, L3 Optimiser, L2 Failure Analyst)`. Labels: `roadmap, agentic, help-wanted`. Sections: Motivation (three agent personas, A2A protocol, ADK on GCP) / Sub-tasks (per-agent) / Acceptance criteria / Open questions (security guardrails, IAM scopes, ADK choice) / Links.

**`c3-markov-prediction.md`** — Title: `C3: Markov-chain prediction of cluster + job state`. Labels: `roadmap, statistics, help-wanted`. Sections: Motivation (state-transition prediction; scenario simulation) / Sub-tasks (state matrix from observed log-analytics signals; transition probabilities; predictor; simulator) / Acceptance criteria / Open questions / Links.

### Smaller follow-ups — ~80 words each

| File | Title | Source | Labels |
|---|---|---|---|
| `tighten-disable-syntax.md` | Tighten scalafix `DisableSyntax` (re-enable `noReturns` + `noWhileLoops`) | SP-1 | `enhancement, code-quality` |
| `scala-test-compile-id-rename.md` | Rename `serve` profile's `scala-test-compile` execution id | SP-1 | `bug, build` |
| `mvnw-script-only.md` | Upgrade Maven wrapper to script-only flavour (drop bundled jar) | SP-1 | `enhancement, build` |
| `gitattributes-line-endings.md` | Add `.gitattributes` for `mvnw` / `mvnw.cmd` line-endings (Windows protection) | SP-1 | `enhancement, build, windows` |
| `maven-central-publish.md` | Maven Central publishing pipeline (Sonatype + GPG) | SP-1 | `enhancement, release` |
| `coverage-no-regression-gate.md` | Coverage no-regression gate on PRs | SP-1 | `enhancement, ci` |
| `branch-protection.md` | Branch protection rules for `main` | SP-1 | `enhancement, repo-config` |
| `logo-design.md` | Logo / brand identity | SP-2 | `design, help-wanted` |
| `arr-dead-statement-cleanup.md` | Drop orphaned `arr(...)` expression on `GenerationSummary.scala:146` | SP-1 | `cleanup` |
| `serve-sh-windows.md` | `serve.sh` Windows portability | SP-2 | `enhancement, windows` |
| `landing-code-syntax-highlighting.md` | Landing page: code-block syntax highlighting (highlight.js or prism.js) | SP-2 | `enhancement, frontend` |

Each file: short Motivation paragraph (where it came from + why it matters) + 1-2 sub-tasks + Acceptance criteria + Effort estimate (S/M/L) + Reference link to the line/file/spec where it was first surfaced.

## Rollout sequence (5 commits in the SP-3 PR + 3 post-merge manual steps)

### In the PR

1. **Add `CONTRIBUTING.md`** (~250 lines, all 12 sections per outline above). The meatiest commit. Independent verification: `./mvnw -B verify` still passes; all deep-link targets in CONTRIBUTING.md resolve.
2. **Add `CODE_OF_CONDUCT.md`** — Contributor Covenant 2.1 verbatim with email substitution.
3. **Add `.github/PULL_REQUEST_TEMPLATE.md` + `.github/ISSUE_TEMPLATE/*.yml`** (5 files: 1 PR template + 3 issue templates + chooser config).
4. **Add `.github/issues/` directory with 14 markdown stubs.** Two-commit split if it gets large: (4a) 3 big initiative files, (4b) 11 smaller follow-up files. Default to one commit unless review feedback prefers split.
5. **Update `README.md` §10 Contributing** — replace SP-2's placeholder mini-guidance with real CONTRIBUTING + CoC + Discussions links.

### Post-merge (NOT in the PR)

6. **Manual: enable Discussions on the repo** (Settings → General → Features → Discussions → check the box). Required for `config.yml`'s Q&A redirect to work. ~10 seconds.
7. **Manual: open all 14 Issues** via `gh issue create --title "..." --body-file .github/issues/<file>.md --label "..."`. ~10-15 minutes.
8. **Follow-up commit:** update `ROADMAP.md` replacing each `[#TBD]` with real issue numbers. Small PR (or push to main with a single squash commit if branch protection allows direct push).

## Verification gates (must pass before SP-3 PR opens)

1. `./mvnw -B verify` succeeds — pure regression check (zero Scala/Maven changes expected).
2. `./mvnw -B spotless:check` and `./mvnw -B compile test-compile scalafix:scalafix -Dscalafix.mode=CHECK` both green — no Scala source changes, but verify nothing slipped.
3. `./mvnw -B -Pserve package` produces the slim jar — frontend regression check.
4. `./serve.sh` boots cleanly and dashboard + landing both render — frontend regression check.
5. CONTRIBUTING.md renders as Markdown without broken links. All deep-link targets (`_DESIGN.md`, `_AUTO_TUNING.md`, `_REFINEMENT.md`, `_LOG_ANALYTICS.md`, `LICENSE`, `CODE_OF_CONDUCT.md`, README sections) resolve.
6. CODE_OF_CONDUCT.md has `alberto.lopez@email.com` substituted (no Contributor Covenant `[INSERT CONTACT METHOD]` placeholder remaining).
7. `.github/ISSUE_TEMPLATE/*.yml` files parse as valid YAML AND validate against GitHub's issue-form schema (basic sanity: `name`, `description`, `body` keys).
8. `.github/PULL_REQUEST_TEMPLATE.md` renders correctly (visual check on a draft PR).
9. All 14 markdown stubs in `.github/issues/` are well-formed Markdown with the YAML-comment-block `gh issue create` invocation hint at the top, plus the standard sections (Title, Motivation, Sub-tasks/Acceptance criteria, Effort/Open questions, Links).
10. README §10 Contributing has been updated — links to CONTRIBUTING.md (not "coming in SP-3") and references CoC + Discussions.

## Risks

- **CONTRIBUTING.md drift** — playbooks (§4-§7) go stale if the underlying code changes. Mitigation: link out to `_DESIGN.md` etc. for the source-of-truth; describe only the workflow at a stable level (clone, override, register, test). Add a SP-3 follow-up Issue if drift becomes painful.
- **Discussions not enabled at PR merge time** — `config.yml` redirect points to a 404. Mitigation: PR description explicitly flags "enable Discussions before merge OR immediately after." Repository owner = same person opening the PR, so the gap is tight.
- **`gh issue create` command fragility** — if the user runs it from the wrong dir, label names don't match repo labels, etc., issues open malformed. Mitigation: the YAML-comment-block in each `.github/issues/*.md` carries the exact command; copy-paste-able.
- **Issue body bodies go stale before opening** — between PR merge and the gh-issue-create session, if the codebase changes the bodies could reference moved code. Mitigation: aim to open issues within 24 hours of merge.
- **The 14 placeholder Issues clutter the Issue tracker on day one** — a fresh repo with 14 open Issues looks busy. Mitigation: that's the POINT — `[#TBD]` placeholders in ROADMAP need real numbers; visitors clicking through see real entries; "good first issue" labels make some claimable immediately.

## Dependencies on other sub-projects

- **SP-2 (already merged):** SP-3 depends on `ROADMAP.md` existing with the 14 `[#TBD]` placeholders. Both landed in PR #11.
- **SP-4 (Medium articles):** independent of SP-3; can run in parallel after SP-3 merges.

## Open questions (resolve at plan-write time)

1. **`gh issue create --title "..." --body-file <file>` invocation hint format inside each .md stub** — HTML comment block with the full command (per the C1 example above) vs YAML-style frontmatter parsed by a wrapper script. Recommend: HTML comment block, copy-paste-friendly, no parser needed.

2. **`.github/issues/` vs `.github/ISSUE_BODIES/` directory naming** — `.github/issues/` is conventional but visually similar to GitHub's own Issues UI label. `.github/ISSUE_BODIES/` is more explicit. Recommend: `.github/issues/` (lowercase, conventional).

3. **README.md §10 update — own commit (commit 5) or bundled with CONTRIBUTING.md (commit 1)?** — Recommend own commit (commit 5) for cleaner review of "what content vs what wiring."

4. **CONTRIBUTING §3 architecture orientation: include a tiny Mermaid module diagram?** — A 6-box Mermaid diagram (`single/`, `auto/`, `auto/frontend/`, `log_analytics/`, `utils/spark/cache/`, `utils/spark/parallelism/`) with arrows would be navigational gold. Cost: ~5 lines of Mermaid + readers would render it natively on GitHub. Recommend: yes, add it.

5. **Should the 11 smaller-follow-up Issues each include an Effort estimate (S/M/L)?** — Helps contributors picking. Recommend: yes, add as a one-line field at the bottom of each.

6. **Should a `good first issue` label exist + be applied to a subset of the 14?** — Best candidates: `gitattributes-line-endings.md`, `arr-dead-statement-cleanup.md`, `landing-code-syntax-highlighting.md`. Recommend: yes, apply that label to those three; their Issue stubs note the label in the gh-create command.
