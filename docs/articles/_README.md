# Articles

Long-form drafts targeted at Medium publication, version-controlled in this repo before publishing. Each article is `YYYY-MM-DD-part-N-<slug>.md`.

The articles are part of the OSS-readiness series and complement the technical docs in this repo ([README](/README.md), [`_DESIGN.md`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/single/_DESIGN.md), [`_AUTO_TUNING.md`](/src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/_AUTO_TUNING.md), etc.) by giving the project a narrative arc — what it is, why it exists, where it's going.

## The series

| # | Article | Status |
|---|---|---|
| PART_1 | [F1 telemetry for your Spark cluster](2026-05-09-part-1-telemetry.md) | drafted |
| PART_2 | [From CSV to optimized cluster config in 5 minutes](2026-05-09-part-2-tuners-and-frontend.md) | drafted |
| PART_3 | Results + case studies | TBD — drafts open when real-world numbers materialise |
| PART_4 | [The road from local tuner to autonomous cluster optimisation](2026-05-09-part-4-future-direction.md) | drafted |

## How to publish to Medium

The articles in this dir are GitHub-rendered drafts. Medium requires a few translations at publication time.

1. **Open the article's MD file.** Read it end-to-end one final time.
2. **Replace `💭` voice-slot placeholders** with your actual prose. The hint bullets are suggestions for what kind of anecdote / opinion fits — not a script. Replace freely.
3. **Translate image references.** The MD uses relative paths like `../images/1_hero.png`. Medium needs absolute URLs. Two options:
   - **Manual upload** to Medium's media tool, then paste the Medium-hosted URL into the article.
   - **External URL via GitHub** — change `../images/X.png` → `https://github.com/albertols/spark-cluster-job-tuner/raw/main/docs/images/X.png`. Medium accepts external image URLs.
4. **Mermaid diagrams** — Medium does NOT render Mermaid natively. Either:
   - Render the Mermaid block to SVG/PNG (e.g., via `mermaid-cli`, mermaid.live, or screenshot from the rendered GitHub view), upload as an image.
   - Or describe the diagram in prose if the visual isn't load-bearing.
5. **Copy the entire MD body** to Medium's editor. Medium will mostly do the right thing with headings, lists, code blocks, blockquotes.
6. **Set the canonical URL** in Medium's story settings to point back at the GitHub source for SEO: `https://github.com/albertols/spark-cluster-job-tuner/blob/main/docs/articles/<file>.md`.
7. **Apply the article's tag list** (each article has a `Tags:` line in its top metadata comment).

## The `💭` slot convention

Each article has one `💭` quote block — an explicit drop-in for personal voice. The hint bullets inside are suggestions; replace with whatever banter / anecdote / opinion fits your style. The article reads cleanly even if you leave the slot empty (the slots are bonus voice, not load-bearing).

## Cross-link conventions

Each article's "What's next" section links to the next article in the series (PART_1 → PART_2 → PART_4) plus the repo. PART_4's "What's next" links back to ROADMAP.md and the C1/C2/C3 GitHub Issues for contributors.
