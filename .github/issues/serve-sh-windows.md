<!-- gh issue create
  --title "serve.sh Windows portability"
  --label "enhancement"
  --label "windows"
-->

# serve.sh Windows portability

## Motivation

`serve.sh` is bash + `cp` + `python3 -m http.server`. Works on macOS/Linux; doesn't work natively on Windows. Windows contributors today need WSL or git-bash + python — friction.

## Sub-tasks

- Either: write a parallel `serve.cmd` (PowerShell) that does the same boot flow.
- Or: rewrite `serve.sh` in a portable form (cross-platform Python or a JVM-side launcher).
- Update README §Quickstart with the Windows path.

## Acceptance

A Windows contributor can `./mvnw -Pserve package && serve.cmd` (or equivalent) and reach the dashboard.

## Effort

M (porting the whole serve.sh logic; testing on Windows is the bottleneck)

## Reference

SP-2 spec § Risks — Windows portability was a known SP-3 follow-up.
