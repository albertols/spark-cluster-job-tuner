<!-- gh issue create
  --title "Add .gitattributes for mvnw / mvnw.cmd line-endings (Windows protection)"
  --label "enhancement"
  --label "build"
  --label "windows"
  --label "good first issue"
-->

# Add .gitattributes for mvnw line-endings

## Motivation

Windows contributors with `core.autocrlf=true` will get CRLF in `mvnw`, which breaks the `#!/bin/sh` shebang on WSL/git-bash. A `.gitattributes` file pinning specific line-endings prevents this.

## Sub-tasks

- Create `.gitattributes` with:
  ```
  mvnw            text eol=lf
  mvnw.cmd        text eol=crlf
  *.sh            text eol=lf
  *.scala         text eol=lf
  ```
- Verify on a Windows machine (or CI matrix Windows job — tracked separately).

## Acceptance

`mvnw` always lands on contributor disks with LF line endings regardless of `core.autocrlf` setting.

## Effort

S (~10 lines, one new file)

## Reference

SP-1 T1 quality review surfaced this.
