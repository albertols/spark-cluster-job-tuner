#!/usr/bin/env bash
#
# Serves the auto-tuner dashboard on localhost:8080.
#
# Usage:
#   ./serve.sh                              # serves from frontend/ dir (assumes JSON sits next to index.html)
#   ./serve.sh /path/to/outputs/<curDate>_auto_tuned   # serves from the PARENT of that dir
#                                                       # so the dashboard can also reach ../<refDate>/<cluster>-*.json
#
# The dashboard looks for _auto_tuner_analysis.json in the served directory.
# When an output dir is passed, the launcher serves from its parent and opens
# the browser with ?data=<curDirName>/_auto_tuner_analysis.json so the frontend
# can resolve sibling reference-date JSONs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8080}"

OPEN_URL="http://localhost:$PORT"

if [ $# -ge 1 ]; then
  OUTPUT_DIR="$1"
  if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: Directory not found: $OUTPUT_DIR"
    exit 1
  fi
  OUTPUT_DIR="$(cd "$OUTPUT_DIR" && pwd)"
  PARENT_DIR="$(dirname "$OUTPUT_DIR")"
  CUR_NAME="$(basename "$OUTPUT_DIR")"

  # Copy frontend assets into the output dir (only if missing).
  cp -n "$SCRIPT_DIR/index.html" "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/style.css"  "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/app.js"     "$OUTPUT_DIR/" 2>/dev/null || true

  SERVE_DIR="$PARENT_DIR"
  OPEN_URL="http://localhost:$PORT/$CUR_NAME/?data=_auto_tuner_analysis.json"
else
  SERVE_DIR="$SCRIPT_DIR"
fi

echo "Serving auto-tuner dashboard at $OPEN_URL"
echo "Directory: $SERVE_DIR"
echo "Press Ctrl+C to stop."

if command -v open &>/dev/null; then
  open "$OPEN_URL" &
elif command -v xdg-open &>/dev/null; then
  xdg-open "$OPEN_URL" &
fi

cd "$SERVE_DIR"
python3 -m http.server "$PORT"
