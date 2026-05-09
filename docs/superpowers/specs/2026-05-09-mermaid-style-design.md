# Mermaid Style Upgrade — Design

**Status:** Draft for review
**Date:** 2026-05-09
**Owner:** @albertols
**Bundled into:** SP-3 PR (`oss/sp3-community-infra` branch). NOT a standalone sub-project of OSS readiness; this is a content/style addendum that ships with SP-3.

## Context

The repo has 9 Mermaid diagrams across 4 files (`README.md`, `CONTRIBUTING.md`, `src/main/scala/.../auto/_AUTO_TUNING.md`, `src/main/scala/.../single/refinement/_REFINEMENT.md`). Visual quality varies wildly:

- **`_AUTO_TUNING.md`** has the richest diagram in the repo — `classDef` with 9 categories, subgraph-organised inputs/outputs, emoji icons in node labels. Excellent structure. **Problem:** the colour palette is dark-mode-only (`fill:#1e3a5f` etc.) and reads poorly on light Mermaid theme.
- **`README.md`** has 4 diagrams (telemetry flow, two job-virtualisation subgraphs, boost lifecycle FSM). Plain styling — no `classDef`, no icons beyond `<br/>` text labels.
- **`CONTRIBUTING.md`** has 1 diagram (architecture orientation). Same plain styling.
- **`_REFINEMENT.md`** has 3 diagrams. Diagram 1 has a single inline `style C fill:#d4edda`. Diagrams 2 + 3 have no styling at all.

This design unifies the visual language: a single `MERMAID_STYLE.md` style guide codifies the palette + icon vocabulary + input/output treatment, and an upgrade pass brings the 8 remaining diagrams up to (and refreshes) the `_AUTO_TUNING.md` standard.

## Goals

1. Land `docs/MERMAID_STYLE.md` as the single source of truth for Mermaid style — copy-paste-ready `classDef` block, icon vocabulary, input/output patterns, anti-patterns, theme-compatibility notes.
2. Upgrade all 9 diagrams to follow the style guide. Theme-agnostic colours (works on both Mermaid `default` and `dark` themes).
3. CONTRIBUTING.md gets a 2-line pointer to `MERMAID_STYLE.md` so future contributors find it immediately.
4. Zero perturbation to Scala/Maven; pure docs work.

## Non-goals

- Adding new diagrams (this is a style pass, not a content pass).
- Changing diagram CONTENT, except where structurally needed (the README's two stacked job-virtualisation diagrams may consolidate into one — see "Open questions" below).
- Vendoring Mermaid locally instead of CDN (separate SP-3 follow-up Issue: `landing-code-syntax-highlighting` is in the same neighbourhood).
- Switching diagram TYPES (e.g., the `_REFINEMENT.md` `classDiagram` stays as `classDiagram` — has limited styling but valid).
- Rewriting the `_AUTO_TUNING.md` diagram's structure — it's exemplary; only the palette refactors.

## Decisions (locked in during brainstorming)

| # | Decision | Choice | Rationale |
|---|---|---|---|
| Q1 | Scope shape + branch | **Style guide doc + one-time upgrade pass; bundled into the open SP-3 PR (`oss/sp3-community-infra`)** | User explicitly chose to bundle. Style guide is the durable artifact; the upgrade pass is one-time. |
| Q2 | Icon system | **FontAwesome inline (`fa:fa-folder` etc.) + emoji fallback** | Universal Mermaid 8.x+ support; works on GitHub natively + via mermaid.js@11.4.1 in landing. Architecture-beta would force-rewrite our flowchart + state diagrams as a different type, doesn't fit. |
| Q2 | Colour palette | **6 base categories + 2 boundary modifiers + 3 outcome modifiers** (table below) | 6 base covers all node semantics in our 9 diagrams; outcome modifiers preserve `_AUTO_TUNING.md`'s good/bad/neutral semantic richness. |
| Q3 | Input/output highlighting | **Selective — subgraph wrapper for diagrams with 3+ entry/exit nodes; icon-prefix only for smaller** | Keeps small state diagrams clean while big telemetry-flow gets visual anchoring. |
| Q4 | Style guide location | **`docs/MERMAID_STYLE.md`** | Standalone, easy to extend, CONTRIBUTING.md gets a 2-line pointer. |

## Deliverables

### New files

```
docs/MERMAID_STYLE.md                       ~250-300 lines
```

### Modified files

```
README.md                                    upgrade 4 diagrams (telemetry flow, 2 worker subgraphs → consolidate to 1, boost lifecycle FSM)
CONTRIBUTING.md                              upgrade 1 diagram (architecture orientation) + 2-line pointer to MERMAID_STYLE.md
src/main/scala/.../auto/_AUTO_TUNING.md      palette refactor only (structure stays — it's exemplary)
src/main/scala/.../single/refinement/_REFINEMENT.md   upgrade 3 diagrams
```

### Out of scope

- Per "Non-goals" section above.
- Code-block syntax highlighting in landing (separate SP-3 Issue).
- Switching to mermaid 11+ exclusively (we already use 11.4.1 in landing; GitHub uses an older bundled version — common-denominator syntax stays).

## Style guide architecture (`docs/MERMAID_STYLE.md` outline)

| § | Section | Words |
|---|---|---|
| 1 | **Why this exists** | 80 |
| 2 | **Quick start** (copy-paste classDef block + minimal example) | 80 |
| 3 | **Categories** — 6 base + 3 outcome modifiers — table with Fill / Stroke / Text colour / When to use | 200 |
| 4 | **Icon vocabulary** — table mapping concepts → FontAwesome / emoji | 200 |
| 5 | **Input / output highlighting** — subgraph wrapper vs icon-prefix patterns with code samples | 150 |
| 6 | **Diagram-type cheatsheet** — one canonical example per type: `flowchart LR`, `flowchart TD`, `stateDiagram-v2`, `classDiagram`, `subgraph` nesting | 250 |
| 7 | **Theme compatibility** — palette works on both light + dark Mermaid themes | 80 |
| 8 | **Anti-patterns** — no inline `style X fill:`, don't invent categories, don't mix icon styles | 120 |
| 9 | **Maintenance** — when adding categories or icons | 50 |

Total: ~1200 words → ~250-300 lines after Markdown formatting + classDef code blocks + tables.

## The canonical `classDef` block (locked)

Copy-paste-ready, theme-agnostic. Mid-tone fills + dark strokes that read on BOTH `theme: 'default'` and `theme: 'dark'` Mermaid themes.

```
%% Base categories — apply one per node
classDef document   fill:#9aa2ab,stroke:#3a4046,color:#1d1f23
classDef process    fill:#9b59b6,stroke:#5a2d6e,color:#fff
classDef spark      fill:#ff7a18,stroke:#8a3a00,color:#fff
classDef cloud      fill:#4ea1ff,stroke:#1a4f8a,color:#fff
classDef frontend   fill:#10b981,stroke:#054b34,color:#fff
classDef container  fill:#fef08a,stroke:#7a5e00,color:#1d1f23

%% Outcome modifiers — apply alongside a base for trend / decision visualisation
classDef outcomeGood    fill:#1a3a1a,stroke:#2ecc71,color:#c8f0c8
classDef outcomeBad     fill:#3a1a1a,stroke:#e74c3c,color:#ffc8c8
classDef outcomeNeutral fill:#2a2a3a,stroke:#7f8c8d,color:#d5d8dc

%% Input / output boundary modifiers — compose with a base via :::base:::boundary
classDef inputBoundary  stroke-width:3px,stroke-dasharray:5 3
classDef outputBoundary stroke-width:3px
```

**Composition pattern:** `MyNode[label]:::process:::outputBoundary` — base category + boundary modifier in the same node declaration.

**Special case:** `outcome*` classes are intentionally darker-fill + lighter-text (preserved from `_AUTO_TUNING.md`'s palette) because they signal trend semantics, not node taxonomy. They override the base fill but should still compose with `inputBoundary`/`outputBoundary` if the node is also a boundary.

## Icon vocabulary (locked)

| Concept | FontAwesome | Emoji fallback | When to use |
|---|---|---|---|
| Folder / directory | `fa:fa-folder` | 📁 | Subgraph titles representing dirs (`📁 inputs/<date>/`) |
| CSV file | `fa:fa-file-csv` | 📄 | Document nodes named `*.csv` |
| SQL file | `fa:fa-database` | 🗄️ | bNN.sql nodes |
| JSON file | `fa:fa-file-code` | 📋 | Output JSON nodes |
| Markdown / text | `fa:fa-file-alt` | 📝 | README, docs, log output |
| Process / Scala class | `fa:fa-cog` | ⚙️ | Tuner / Refinement / generic processing |
| Spark / executor | `fa:fa-bolt` | ⚡ | Spark App, ExecutorTrackingListener |
| Dashboard / landing | `fa:fa-desktop` | 🖥️ | UI surfaces |
| Cloud / GCP service | `fa:fa-cloud` | ☁️ | Log Analytics, BigQuery, GCS, Dataproc |
| Date / snapshot | `fa:fa-calendar` | 🗓️ | DateSnapshot, ref/current |
| Statistical analysis | `fa:fa-chart-bar` | 📊 | TrendDetector, StatisticalAnalysis |
| State / lifecycle | (use stateDiagram-v2 syntax — no icon) | — | Boost lifecycle states |
| Worker / VM | `fa:fa-server` | 🖧 | Container subgraphs (N2-32 etc.) |
| Decision / branch | `fa:fa-code-branch` | 🔀 | Decision diamonds in flowcharts |
| Pair / link | `fa:fa-link` | 🔗 | MetricsPair, snapshot pairing |
| Input boundary prefix | — | 📥 | Entry node label prefix |
| Output boundary prefix | — | 📤 | Exit node label prefix |

**Strict rule:** one diagram, one icon style. All FA OR all emoji within a single diagram. Mixing only allowed when a specific concept has no FA equivalent (e.g., 📥/📤 input/output prefixes).

## Input/output treatment (per Q3)

**Subgraph wrapper pattern** — for diagrams with 3+ entry or exit nodes (e.g., the README telemetry flow, the `_AUTO_TUNING.md` diagram):

```
subgraph INPUTS ["📥 Inputs (BigQuery exports)"]
  B13[fa:fa-file-csv b13.csv]:::document
  B14[fa:fa-file-csv b14.csv]:::document
  B16[fa:fa-file-csv b16.csv]:::document
end
class B13,B14,B16 inputBoundary
```

**Icon-prefix pattern** — for diagrams with 1-2 entry/exit nodes (e.g., the boost lifecycle FSM, the `_REFINEMENT.md` simple flowcharts):

```
A[📥 Snapshot read]:::process:::inputBoundary
Z[📤 JSON output]:::document:::outputBoundary
```

**Decision rule:** count the entry/exit nodes in the diagram. ≥3 → wrapper. ≤2 → icon prefix.

## Per-diagram upgrade plan (the 9 diagrams)

| File | Diagram | Current state | Action |
|---|---|---|---|
| `README.md` §3 | Telemetry flow (`flowchart LR`) | Plain — `<br/>` text labels, no classDef | Add classDef, replace text annotations with FA/emoji icons, wrap inputs in `subgraph INPUTS` (3 source nodes: ExecutorTrackingListener, cluster events, autoscaler) + outputs in `subgraph OUTPUTS` (Tuner → Dashboard) |
| `README.md` §3 | Job virtualisation (2× `flowchart TB` subgraphs, currently stacked) | Two separate Mermaid blocks rendering as stacked diagrams | **Consolidate into 1 diagram** with `flowchart LR` outer + two `subgraph` blocks (N2-32 + E2-32) side by side. Apply `container` class to the worker subgraphs. Inner executor blocks use `process` class with the worker icon. **Fallback if it renders poorly:** keep as two diagrams, just apply classDef + icons. |
| `README.md` §5 #1 | Boost lifecycle FSM (`stateDiagram-v2`) | Has note + transitions; no styling | Apply `classDef` (state diagrams support it via `class StateName className` syntax). Tighten the BoostMetadataCarrier note. |
| `CONTRIBUTING.md` §3 | Architecture orientation (`flowchart LR`) | Plain — `<br/>` text labels, no classDef | Add classDef, replace text labels with FA icons (folder + cog + spark + cloud), keep dotted edges (`-. .->`) for the supporting modules (`cache/`, `parallelism/`). |
| `_AUTO_TUNING.md` | The big multi-subgraph flowchart | Excellent structure; dark-only palette (9 classDef categories) | **Palette refactor ONLY.** Replace `classDef csvFile fill:#1e3a5f...` etc. with the canonical 6-category palette + outcome modifiers. Map: csvFile → document, scalaObj → process, snapshot → process, decision → container OR outcomeNeutral, actionGood → outcomeGood, actionBad → outcomeBad, actionNeutral → outcomeNeutral, outJson → document:::outputBoundary, outCsv → document:::outputBoundary, outTxt → document:::outputBoundary, dirBox → container. **Structure unchanged** — every existing node + edge stays. |
| `_REFINEMENT.md` | Diagram 1 (`flowchart LR` — Base Tuner + Refinement Layer) | Minimal styling, one inline `style C fill:#d4edda` | Add classDef, remove inline `style`, apply: BigQuery → cloud, ClusterMachineAndRecipeTuner → process, JSON outputs → document:::outputBoundary, b16_oom.csv → document:::inputBoundary, RefinementPipeline → process. |
| `_REFINEMENT.md` | Diagram 2 (`flowchart TD` — RecipeResolver + Vitamins chain) | No styling | Add classDef. Parsed Tuned JSON → document:::inputBoundary, RecipeResolver → process, Vitamin nodes → process, "_not_boosted_recipes.json" → document:::outputBoundary, Refined JSON Output → document:::outputBoundary. |
| `_REFINEMENT.md` | Diagram 3 (`classDiagram`) | No styling — UML class diagram | Apply `class X cssClass` syntax (Mermaid `classDiagram` supports limited class-based styling). RefinementVitamin trait + concrete Vitamin classes get the `process` class colour. Sealed traits (VitaminSignal, VitaminBoost, BoostState) get `outcomeNeutral`. **Acceptable degradation:** `classDiagram` styling is more limited than `flowchart`; some classDef properties may not apply cleanly. Document the limitations in MERMAID_STYLE.md §6. |

**Total:** 9 diagrams across 4 files. Roughly 60-80 nodes touched. ~2-3 hours of focused work.

## Rollout sequence (5 commits)

1. **Add `docs/MERMAID_STYLE.md`** (the style guide — independent commit, no diagram changes yet).
2. **Upgrade README diagrams** (4 diagrams in 1 file). Includes the worker-subgraph consolidation (with fallback to two diagrams if it degrades).
3. **Upgrade `CONTRIBUTING.md`** (1 diagram + 2-line pointer to MERMAID_STYLE.md near §"Architecture orientation").
4. **Refactor `_AUTO_TUNING.md` palette** (most surgical commit — structure unchanged, only `classDef` lines + class assignments swap). Easy to review.
5. **Upgrade `_REFINEMENT.md`** (3 diagrams).

Each commit independently reviewable. Each leaves the project in a working state (no broken Mermaid syntax in any commit boundary).

## Verification gates (must pass before commits land)

1. `./mvnw -B verify` still passes (no Scala changes — pure regression check).
2. Each modified `.md` file's Mermaid blocks parse — visual check via the landing page (`./serve.sh`, browse rendered README) AND via GitHub's Markdown preview (push to draft PR).
3. All 9 diagrams render under BOTH `theme: 'default'` (light) and `theme: 'dark'` (dark) — toggle via `prefers-color-scheme` on the landing.
4. No `style X fill:...` inline declarations remain in any modified file. Verify with: `grep -nE "^\s*style " <file>` — zero hits.
5. `MERMAID_STYLE.md` itself contains a working example of every category — readers can confirm the palette works by viewing the doc rendered on GitHub.
6. CONTRIBUTING.md's pointer to MERMAID_STYLE.md is in place and clickable.

## Risks

- **GitHub's bundled Mermaid version drift** — features like `classDef stroke-dasharray` may render differently on GitHub vs mermaid.js@11.4.1 in our landing. Mitigation: stick to syntax stable since mermaid 8.x — the `classDef` + multi-class composition + state diagram class syntax are all in this safe set.
- **`classDiagram` styling limits** — `classDiagram` (Mermaid's UML class diagram type) has more limited styling than `flowchart`. The 3rd `_REFINEMENT.md` diagram (Vitamin class hierarchy) may not get full classDef treatment. Acceptable degradation; documented in style guide §6.
- **Theme-agnostic colours can look "muddy"** — the trade-off for working on both light + dark backgrounds. If a colour really doesn't work on one theme, fall back to mermaid's `init` directive to set theme-aware variables per diagram (last resort). Mid-tone fills + dark strokes is the empirical sweet spot.
- **Worker-subgraph consolidation may render poorly** on GitHub's older Mermaid — the side-by-side `flowchart LR` outer with two `flowchart TB` subgraphs is valid syntax but visual rendering varies. Fallback is documented in commit 2 of the rollout: if consolidation looks weak, keep as two diagrams.
- **Outcome modifier classes (outcomeGood/Bad/Neutral) preserved from dark-mode palette** — these intentionally have dark fills + light text (different from the 6 base categories). The mismatch is semantic, not a bug — they signal trend outcomes, not taxonomy. Document this prominently in MERMAID_STYLE.md.

## Dependencies on other sub-projects

- **SP-3 (in-flight on this same branch):** this design BUNDLES into the open SP-3 PR. Both ship together when SP-3 merges. The PR description should mention the Mermaid upgrade as a secondary scope alongside the community-infra primary scope.
- **SP-1 + SP-2 (already merged):** SP-2's existing diagrams (in README) and SP-3's just-added CONTRIBUTING.md diagram are the upgrade targets.
- **Future sub-projects:** any new Mermaid diagram in the codebase from now on follows `MERMAID_STYLE.md`. CONTRIBUTING.md's pointer makes this discoverable.

## Open questions (resolve at plan-write time)

1. **`_AUTO_TUNING.md` 9-category collapse** — the existing palette has separate `actionGood` / `actionBad` / `actionNeutral` for trend visualisation (green/red/gray). The design proposes mapping these to `outcomeGood` / `outcomeBad` / `outcomeNeutral` modifiers, preserving the semantic. Verify at plan time that this 1:1 mapping covers all current uses without losing meaning. If a node uses BOTH a base category AND an outcome (e.g., a `process` node that is also `outcomeGood`), Mermaid's multi-class composition should make this work — confirm with a real example before finalising.
2. **Worker-subgraph consolidation in README §3** — try the side-by-side `flowchart LR` with two TB subgraphs first; visually compare to the current two-stacked-diagrams approach. Pick the better one. Plan encodes both branches.
3. **`classDiagram` styling fidelity** — exact subset of classDef properties that `classDiagram` honours. Verify experimentally during plan execution; document caveats in MERMAID_STYLE.md §6.
4. **Container vs frontend colour reuse** — both are bright/distinct. The plan keeps them separate (container = soft amber for grouping; frontend = emerald for UI surfaces). If a future diagram conflates them, revisit.
5. **CONTRIBUTING.md pointer placement** — at the top of §"Architecture orientation" (where the existing Mermaid lives), or as a new sub-section before the diagram. Recommend: 2-line callout immediately above the diagram, e.g., `> 📐 **Mermaid style:** all diagrams in this repo follow [`docs/MERMAID_STYLE.md`](docs/MERMAID_STYLE.md). When adding new diagrams, copy the canonical `classDef` block from there.`
