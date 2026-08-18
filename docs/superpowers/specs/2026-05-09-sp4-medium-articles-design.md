# SP-4 — Medium Articles (PART_1 / PART_2 / PART_4)

**Status:** Draft for review
**Date:** 2026-05-09
**Owner:** @albertols
**Sub-project of:** OSS Readiness epic — final sub-project. SP-1 (build & quality), SP-2 (landing surface), SP-3 (community infra + bundled Mermaid style) all merged to `main`.
**Branch:** `oss/sp4-articles` (off main).

## Context

SP-4 is the long-form editorial closeout of the OSS readiness epic — three Medium-publication-targeted articles drafted as version-controlled markdown in `docs/articles/`. The articles are PART_1 (telemetry), PART_2 (tuners + frontend), PART_4 (future direction). PART_3 (results / case studies) is deferred per the user's earlier direction until real-world numbers are in hand.

The articles serve three audiences in priority order: (1) Spark engineers on GCP Dataproc evaluating the tool, (2) potential OSS contributors, (3) the broader data-engineering Medium readership. They reference SP-1's quality bar, SP-2's landing visuals, and SP-3's community infra + ROADMAP.md as their content scaffolding — the codebase's existing assets do most of the heavy lifting visually; the articles add the editorial framing.

The user (@albertols) will layer personal voice / war stories / opinions on top via explicit `💭` drop-in slots in each article. Implementation drafts those slots with 2-3 hint bullets but does NOT write the user's voice for them.

## Goals

1. Land 3 publication-ready Medium articles in `docs/articles/` as version-controlled markdown drafts.
2. Each article ~1500 words (PART_2 may extend to ~2500w if surface area demands).
3. Consistent skeleton across all 3: TL;DR + hook + 3 body sections + 💭 voice slot + What's next.
4. Reuse README.md's 5 PNG screenshots + 3 Mermaid diagrams (zero new image asset production except 1 new C1/C2/C3 architecture Mermaid for PART_4).
5. `docs/articles/_README.md` documents how to publish to Medium (image URL translation, canonical URL, tag conventions).
6. Zero code/build perturbation (pure docs).

## Non-goals (deferred)

- **PART_3** (results / case studies) — deferred per user direction.
- Medium publication itself — editorial workflow, not engineering. The PR delivers the drafts; the user publishes.
- Cross-promotion copy (Twitter / LinkedIn announcement snippets) — separate small task at publish time.
- Reformatting existing repo docs (README, `_*.md`) to point at the articles — articles link OUT to the repo, not the inverse.
- New images / re-screenshots — articles reuse what SP-2 already delivered.

## Decisions (locked in during brainstorming)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| Q1 | Sub-project shape + branch + PR cadence | **One SP-4 sub-project, one `oss/sp4-articles` branch, one PR with all 3 articles. Drafts at `docs/articles/`.** | Shared voice/vocabulary stays consistent; reviewer sees the whole arc; minimal coordination overhead. |
| Q2 | Voice + length | **Hybrid voice (technical neutral prose + explicit `💭` drop-in slots for user's banter); ~1500w per article (PART_2 may extend to 2500w).** | User wants to layer personality on top — drop-in slots make the seam explicit. Concise length keeps each article in Medium's sweet spot for 5-7 min reads. |
| Q3 | Visuals strategy | **Reuse README's 5 PNGs + 3 Mermaid diagrams. PART_4 adds 1 new C1/C2/C3 architecture Mermaid as inline content.** | Zero asset production overhead. Same visual language as README builds reader trust. PART_4 is forward-looking so the new diagram is illustrative, not architectural truth. |

## Per-article template (consistent across all 3)

```markdown
<!-- Medium publication metadata
  Title:    [final title]
  Subtitle: [final subtitle]
  Tags:     spark, gcp, dataproc, data-engineering, [article-specific tag]
  Canonical URL: https://github.com/albertols/spark-cluster-job-tuner/blob/main/docs/articles/<file>.md
-->

# [Title]

> [One-sentence hook subtitle]

![Hero image](../images/<chosen-hero>.png)
<!-- For Medium: replace with absolute URL https://github.com/albertols/spark-cluster-job-tuner/raw/main/docs/images/<chosen-hero>.png -->

## TL;DR

- [Bullet 1 — punchy one-liner]
- [Bullet 2]
- [Bullet 3]

## [Hook section — ~200w]

[Sets up why this matters NOW. Concrete pain.]

## [Body section 1 — ~400w]

## [Body section 2 — ~400w]

## [Body section 3 — ~300w]

> 💭 **[Your voice goes here]**
>
> Optional anecdote / opinion / war-story slot. Keep or replace with your banter. Suggestions for what to write:
> - [hint 1]
> - [hint 2]
> - [hint 3]

## What's next

[Brief recap + link to next article in the series + link to repo. ~100w]

---

*If this was useful: ⭐ the repo, follow me, share your war stories in a comment.*
```

Total budget per article: ~1500w of substance + 1 voice slot + standard CTAs.

## PART_1 outline — Telemetry: the bNN files and where they come from

**Filename:** `docs/articles/2026-05-09-part-1-telemetry.md`
**Title:** *F1 telemetry for your Spark cluster: the BigQuery Log Analytics setup that costs nothing*
**Subtitle:** *How a tiny Spark listener + 5 GCP Log Analytics queries replace expensive BigQuery export pipelines for cluster sizing.*
**Hero:** `../images/1_hero.png` (the dashboard).
**Tags:** `spark, gcp, dataproc, data-engineering, observability`

**Sections:**

1. **TL;DR** — (1) ExecutorTrackingListener emits structured executor logs your Spark app already runs. (2) Five Log Analytics queries (b13/b14/b16/b20/b21) extract enough signal to size clusters intelligently. (3) Log Analytics costs less than equivalent BigQuery exports for this workload — same data, cheaper.
2. **Hook (~200w)** — *The cost-vs-blindness trade-off.* Most teams either over-provision (visible cost) or fly blind (hidden cost — failed jobs, slow scale-up, OOMs). What if you could measure cheaply?
3. **Body 1 (~400w) — The two telemetry sources:**
   - GCP-native logs (`cloud_dataproc_job` + `dataproc.googleapis.com/autoscaler`) — automatic, free, but coarse.
   - `ExecutorTrackingListener` — a `SparkListener` you wire into your app via `spark.extraListeners=...`. Emits structured JSON for executor add/remove/state-change. F1 telemetry for your Spark engine.
   - **Diagram:** README §3 telemetry-flow Mermaid (reuse — same source).
4. **Body 2 (~400w) — The 5 queries (b13/b14/b16/b20/b21):** what each captures in plain language. Reference SP-2 T7's structured SQL header template (Purpose / Telemetry / GCP source / App source / Consumed) — same vocabulary so readers can navigate the actual SQL files.
5. **Body 3 (~300w) — Cost angle: Log Analytics vs BigQuery:** Log Analytics' per-query pricing model is cheaper than equivalent BigQuery export queries for the cluster-tuning use case (small intermittent reads, no joins, no aggregations beyond GROUP BY). Concrete numbers if available — otherwise framed as "an order of magnitude cheaper for this workload."
6. **💭 voice slot** with hint bullets:
   - The cost-discovery story / "we burned €X on BQ exports before realising Log Analytics existed."
   - Why ExecutorTrackingListener exists at all — what was the alternative you ruled out?
   - The "F1 telemetry" framing — your origin story for it.
7. **What's next** — link to PART_2 (tuner internals) + repo + CTA to wire ExecutorTrackingListener into your own Spark app.

## PART_2 outline — The tuners + the dashboard

**Filename:** `docs/articles/2026-05-09-part-2-tuners-and-frontend.md`
**Title:** *From CSV to optimized cluster config in 5 minutes: the tuner that reads your Spark history*
**Subtitle:** *Single-date tuner, multi-snapshot auto-tuner, statistical boost lifecycle, and a dashboard that shows the math.*
**Hero:** `../images/5_autoscaling.png` (cost & autoscaling lens — the killer screenshot).
**Tags:** `spark, gcp, dataproc, data-engineering, cluster-tuning`

**Sections:**

1. **TL;DR** — (1) Single Tuner picks a machine + executor topology per recipe from one date's metrics. (2) Auto-Tuner pairs reference + current snapshots and detects trends + boost cycles. (3) Z-score executor scale-up + Pearson correlations show you whether the cluster is actually tuned.
2. **Hook (~200w)** — From the b*.csv files (PART_1) to actionable JSON config blocks. The pipeline.
3. **Body 1 (~350w) — Single Tuner:** machine selection (N2-32 > N2D-32 > E2-32 priority + topology preset). Manual vs auto-scale modes. **Diagram:** README's worker-virtualization Mermaid (reuse).
4. **Body 2 (~400w) — Auto-Tuner & boost lifecycle:** snapshot pairing, trend classification, the `New / Holding / ReBoost` FSM. **Diagram:** README's boost-lifecycle FSM Mermaid (reuse). Why state preservation across replans matters (`BoostMetadataCarrier`).
5. **Body 3 (~350w) — Z-score scale-up + the Statistical Lens:** the "inception" — when a recipe is a duration outlier AND cap-touching (`p95_run_max_executors / maxExecutors ≥ 0.5`), raise `spark.dynamicAllocation.maxExecutors` ×1.5 by default. **Visuals:** `2_z-score-cap-touch.png`, `3_trends.png`, `6_pearson_correlation.png` inline as smaller thumbnails.
6. **💭 voice slot** with hint bullets:
   - War story — a specific cluster you tuned, the cost saved (€/month), the unexpected behaviour the math caught.
   - The "aha" moment when the boost lifecycle clicked.
   - Why z-score cap detection beats threshold rules.
7. **What's next** — link to PART_4 (future direction) + repo + CTA to clone + run with sample data.

**Length authorisation:** if 1500w cannot fit cleanly given the surface area, the spec authorises extension to ~2000-2500w. Document the choice in the implementer's commit message.

## PART_4 outline — Where this goes next

**Filename:** `docs/articles/2026-05-09-part-4-future-direction.md`
**Title:** *The road from local tuner to autonomous cluster optimisation*
**Subtitle:** *GCP-deployable, specialised agents, Markov-chain prediction — the roadmap.*
**Hero:** new C1/C2/C3 architecture Mermaid (drafted inline; not a separate image file).
**Tags:** `spark, gcp, dataproc, data-engineering, roadmap`

**Sections:**

1. **TL;DR** — (1) Local-only today, scheduled-on-GCP next. (2) Three agent personas wrap the tuner: Tuner Proposal, L3 Optimiser, L2 Failure Analyst. (3) Markov chains turn reactive trend analysis into predictive forecasting.
2. **Hook (~150w)** — Where the tool is today vs where it's headed.
3. **Body 1 (~400w) — C1 GCP-deployable:** Cloud Run frontend, scheduled BigQuery exports, GCS cache, Terraform IaC. The shape; not the implementation. Link to GitHub Issue C1 (SP-3 will have opened it by SP-4 publication time).
4. **Body 2 (~450w) — C2 Specialised agents:** the three personas, A2A protocol, why agentic instead of "just bigger ML." Honest about where this is speculative. Link to GitHub Issue C2.
5. **Body 3 (~350w) — C3 Markov-chain prediction:** state-transition matrices over the bNN signals, point predictions + scenario simulation. How it complements (not replaces) the deterministic AutoTuner. Link to GitHub Issue C3.
6. **💭 voice slot** with hint bullets:
   - Your vision — what success looks like in 12 months.
   - What kinds of contributors you want (specific skills / interests).
   - Why this isn't being built solo.
7. **What's next** — link to ROADMAP.md + the specific GitHub Issues (C1/C2/C3) for contributors + CTA to file new ideas in Discussions.

**The new C1/C2/C3 Mermaid** (inline in PART_4 body) follows MERMAID_STYLE.md canonical palette + icon vocabulary. Layout: 3 boxes (one per initiative) connected to a central "Tuner core" box, with notes hinting at the technology stack (Cloud Run, Vertex AI Agent Builder, scipy/numpy for Markov). Illustrative, not architectural truth.

## `docs/articles/_README.md` (~80 lines)

Brief index doc. Sections:

1. **What `docs/articles/` is for** — long-form drafts version-controlled before Medium publication. Each article is a `YYYY-MM-DD-part-N-<slug>.md`.
2. **The series** — list of articles + their status (drafted / published / draft-pending). Link table.
3. **How to publish to Medium** — step-by-step:
   - Open the article's MD file.
   - Replace `💭` voice-slot placeholders with your actual prose.
   - Translate image references: change `../images/X.png` → `https://github.com/albertols/spark-cluster-job-tuner/raw/main/docs/images/X.png` (Medium accepts external image URLs).
   - Copy entire MD to Medium editor; tweak formatting if needed (Medium has its own opinions).
   - Set canonical URL to point back at the GitHub source for SEO.
   - Apply the article's tag list.
4. **The `💭` slot convention** — explicit drop-in for personal voice. Hint bullets are suggestions; replace freely.
5. **Cross-link conventions** — each article's "What's next" links to the next in the series + the repo.

## Rollout sequence (5 commits)

1. **Add `docs/articles/_README.md`** — the index doc explaining the dir.
2. **Add `2026-05-09-part-1-telemetry.md`** (~1500w).
3. **Add `2026-05-09-part-2-tuners-and-frontend.md`** (~1500w; extension authorised if needed — flag in commit message).
4. **Add `2026-05-09-part-4-future-direction.md`** (~1500w) — also adds the new C1/C2/C3 Mermaid as inline content.
5. **Cross-link pass** — each article's "What's next" section links to the next article + the repo. Tighten any TL;DRs based on the full set being visible. Update `_README.md`'s status table now that all 3 are in place.

Each commit independently reviewable; each leaves the project in a working state.

## Verification gates

1. `./mvnw -B verify` still passes (regression check — no Scala changes expected).
2. Each article is between 1300-1800 words (300w tolerance on the 1500 target). PART_2 may exceed up to 2500w if needed.
3. Each article has the consistent template structure (TL;DR + hook + 3 body sections + 💭 slot + What's next).
4. Each `💭` slot has 2-3 hint bullets so the user knows what banter would fit there.
5. All image references are relative paths (`../images/X.png`). The `_README.md` documents the Medium-translation step.
6. All deep links (to README sections, `_*.md` design docs, ROADMAP.md, specific GitHub Issues from SP-3) resolve.
7. No factual errors — claims about CLI flags, Scala identifiers, GCP service names, file paths checked against actual code (same rigor as SP-2 T5's fix-up pass).
8. Each article reads cleanly end-to-end without the `💭` slot needing to be filled to make sense (the slots are bonus voice, not load-bearing).
9. PART_4's new C1/C2/C3 Mermaid follows MERMAID_STYLE.md canonical palette + uses the GitHub-compatible `class X modifier` separate-line pattern (NOT inline `:::a:::b` composition — verified during the SP-3 Mermaid fix-up).
10. `docs/articles/_README.md` exists with the publication-mechanics instructions.

## Risks

- **PART_2 1500w is genuinely tight** for the surface area (single tuner + auto-tuner + frontend + boost lifecycle + z-score). Spec authorises extension to 2000-2500w if the implementer can't fit cleanly.
- **`💭` voice-slot hints** could be too prescriptive (user feels boxed in) or too vague (user doesn't know what to write). Sweet spot: 2-3 specific suggestions of what kind of anecdote would land here, NOT a script. The hints START WITH the most concrete suggestion (e.g., "the time we burned €X on BQ exports") to invite specific stories.
- **Medium publication mechanics** are downstream of this PR — image URL translation, canonical URL setup, tag selection are user's job at publish time. The PR is the draft + the `_README.md` instructions.
- **Forward-looking PART_4 claims** could overpromise. Stick to "this is the architecture sketch / open question" framing — not "this will exist by Q4." Each section explicitly frames C1/C2/C3 as "proposed" with link to the (now-open) GitHub Issues.
- **Mermaid GitHub-compatibility** — PART_4's new C1/C2/C3 diagram MUST follow the SP-3 Mermaid fix-up's GitHub-compatible pattern (no inline multi-class composition). Documented in MERMAID_STYLE.md.

## Dependencies on other sub-projects

- **SP-1 + SP-2 + SP-3 (all merged):** SP-4 articles consume SP-2's images + Mermaid diagrams + README structure as their visual content; reference SP-3's CONTRIBUTING.md + ROADMAP.md + (post-SP-3-Issues-creation) the actual GitHub Issue numbers for C1/C2/C3.
- **SP-3's manual post-merge step** — opening the 14 GitHub Issues — should ideally happen BEFORE SP-4 PART_4 publication so the C1/C2/C3 links resolve to real issue numbers, not 404s. If SP-3 Issues aren't yet opened by the time PART_4 publishes, link to ROADMAP.md as fallback.

## Open questions (resolve at plan-write time)

1. **PART_4's new C1/C2/C3 Mermaid level of detail** — my pick: 3 initiative boxes connected to a central "Tuner core," with notes hinting at the tech stack (Cloud Run, Vertex AI Agent Builder, scipy/numpy). Illustrative, not architectural truth. Resolved at draft time.
2. **Article timestamps in filenames** — `2026-05-09` matches today; Medium will use its own publication date. Filename date is repo audit only. No publication-time renaming needed.
3. **Tag list** — Medium tags affect discoverability. Per-article tags locked in the outlines above. Confirm at draft time.
4. **PART_4 link targets for C1/C2/C3 issues** — at draft time, check whether SP-3 has opened the 14 GitHub Issues yet. If yes, link to specific issue numbers. If no, link to `ROADMAP.md` and update post-Issues with a small follow-up commit.
5. **PART_3 (results / cases) reservation** — should the `_README.md` series table list PART_3 as "Coming when real-world numbers are available" or omit it entirely? My pick: list it as "TBD — drafts open when case studies materialise" so readers know the series is intentionally 4-part.
