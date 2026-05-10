<!-- gh issue create
  --title "Landing page: code-block syntax highlighting"
  --label "enhancement"
  --label "frontend"
  --label "good first issue"
-->

# Landing page: code-block syntax highlighting

## Motivation

The new landing page (`auto/frontend/index.html`) renders the README via marked.js + mermaid.js. Code blocks render as plain monospace — readable but not polished. Adding highlight.js or prism.js gives Scala / bash / YAML / Markdown blocks proper syntax colours.

## Sub-tasks

- Pick highlight.js (auto-detect language) or prism.js (declared via class).
- Load from CDN matching the marked.js + mermaid.js pattern (or vendor locally — see related SP-3 follow-up).
- Hook into marked's `highlight` option or post-process `<code>` blocks in the landing's JS.
- Verify all README code blocks render with appropriate colours; verify no regression on Mermaid blocks.

## Acceptance

`./src/main/scala/com/db/serna/orchestration/cluster_tuning/auto/frontend/serve.sh`, open the landing — Scala code in §7 ("Extending it") shows keyword/identifier colouring; bash blocks too.

## Effort

S (one CDN script tag + ~5 lines of glue)

## Reference

SP-2 spec § Non-goals — explicitly deferred from SP-2.
