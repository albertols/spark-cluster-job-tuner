#!/usr/bin/env bash
#
# Serves the auto-tuner dashboard on localhost:8080.
#
# Usage:
#   ./serve.sh
#       Reads ./config.json next to this script, resolves outputsPath, and
#       serves from either this frontend dir (when outputs is inside it) or
#       the common ancestor of the two paths (typical — so /<frontend-name>/
#       and /<outputs-rel>/ are both HTTP-reachable). Opens the landing
#       page in the browser.
#
#   ./serve.sh /path/to/outputs/<curDate>_auto_tuned
#       Back-compat: serves from the PARENT of that dir and opens
#       ?data=_auto_tuner_analysis.json, skipping the landing.
#
# The dashboard reads config.json for outputsPath, then discovers every
# _auto_tuner_analysis.json under it (via _analyses_index.json, or a
# python http.server HTML-listing fallback).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8080}"

# ─── Back-compat: explicit output dir ───────────────────────────────────────
if [ $# -ge 1 ]; then
  OUTPUT_DIR="$1"
  if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: Directory not found: $OUTPUT_DIR"
    exit 1
  fi
  OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
  PARENT_DIR="$(dirname "$OUTPUT_DIR")"
  CUR_NAME="$(basename "$OUTPUT_DIR")"

  cp -n "$SCRIPT_DIR/index.html"  "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/style.css"   "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/app.js"      "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/config.json" "$OUTPUT_DIR/" 2>/dev/null || true

  SERVE_DIR="$PARENT_DIR"
  OPEN_URL="http://localhost:$PORT/$CUR_NAME/?data=_auto_tuner_analysis.json"

  echo "Serving auto-tuner dashboard at $OPEN_URL"
  echo "Directory: $SERVE_DIR"
  echo "Press Ctrl+C to stop."

  if command -v open &>/dev/null;      then open "$OPEN_URL" &
  elif command -v xdg-open &>/dev/null; then xdg-open "$OPEN_URL" &
  fi
  cd "$SERVE_DIR"
  exec python3 -m http.server "$PORT"
fi

# ─── Default: config-driven landing ─────────────────────────────────────────
CONFIG_FILE="$SCRIPT_DIR/config.json"
if [ ! -f "$CONFIG_FILE" ]; then
  echo "Error: config.json not found next to serve.sh: $CONFIG_FILE"
  exit 1
fi

# Use python to parse config.json and resolve paths relative to the config.
read -r OUTPUTS_DIR < <(
  python3 - "$CONFIG_FILE" <<'PY'
import json, os, sys
cfg_path = sys.argv[1]
with open(cfg_path) as f:
    cfg = json.load(f)
outputs_rel = cfg.get("outputsPath")
if not outputs_rel:
    print("Error: outputsPath missing from config.json", file=sys.stderr)
    sys.exit(2)
cfg_dir = os.path.dirname(os.path.abspath(cfg_path))
outputs_abs = os.path.abspath(os.path.join(cfg_dir, outputs_rel))
print(outputs_abs)
PY
)

if [ -z "${OUTPUTS_DIR:-}" ]; then
  echo "Error: failed to resolve outputsPath from $CONFIG_FILE"
  exit 1
fi

# Pick the served directory:
#   - if OUTPUTS_DIR is inside SCRIPT_DIR → serve from SCRIPT_DIR
#   - else                                 → serve from the common ancestor
case "$OUTPUTS_DIR/" in
  "$SCRIPT_DIR"/*)
    SERVE_DIR="$SCRIPT_DIR"
    OPEN_URL="http://localhost:$PORT/"
    ;;
  *)
    # Longest shared prefix ending at a path separator.
    a="$SCRIPT_DIR"
    b="$OUTPUTS_DIR"
    common=""
    IFS='/' read -r -a A <<<"$a"
    IFS='/' read -r -a B <<<"$b"
    n=${#A[@]}
    [ "${#B[@]}" -lt "$n" ] && n=${#B[@]}
    for ((i=0;i<n;i++)); do
      if [ "${A[$i]}" = "${B[$i]}" ]; then
        common="$common/${A[$i]}"
      else
        break
      fi
    done
    # Strip leading empty token from the '/' split.
    SERVE_DIR="${common#/}"
    SERVE_DIR="/$SERVE_DIR"
    # Trim trailing slash duplication if any.
    SERVE_DIR="$(cd "$SERVE_DIR" && pwd)"
    REL_FRONTEND="${SCRIPT_DIR#$SERVE_DIR/}"
    OPEN_URL="http://localhost:$PORT/$REL_FRONTEND/"
    ;;
esac

echo "Serving auto-tuner landing page at $OPEN_URL"
echo "Directory:    $SERVE_DIR"
echo "Outputs root: $OUTPUTS_DIR"
echo "Press Ctrl+C to stop."

if command -v open &>/dev/null;      then open "$OPEN_URL" &
elif command -v xdg-open &>/dev/null; then xdg-open "$OPEN_URL" &
fi

cd "$SERVE_DIR"
exec python3 -m http.server "$PORT"
