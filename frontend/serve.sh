#!/usr/bin/env bash
#
# Serves the auto-tuner dashboard on localhost:8080.
#
# Usage:
#   ./serve.sh                              # serves from frontend/ dir
#   ./serve.sh /path/to/outputs/date_dir    # serves with analysis data from specific output dir
#
# The dashboard will look for _auto_tuner_analysis.json in the served directory.
# You can also pass ?data=path/to/file.json as a URL parameter.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8080}"

if [ $# -ge 1 ]; then
  # Serve from a specific output directory — copy frontend files there
  OUTPUT_DIR="$1"
  if [ ! -d "$OUTPUT_DIR" ]; then
    echo "Error: Directory not found: $OUTPUT_DIR"
    exit 1
  fi
  # Copy frontend files to output dir (preserve originals)
  cp -n "$SCRIPT_DIR/index.html" "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/style.css" "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/app.js" "$OUTPUT_DIR/" 2>/dev/null || true
  SERVE_DIR="$OUTPUT_DIR"
else
  SERVE_DIR="$SCRIPT_DIR"
fi

echo "Serving auto-tuner dashboard at http://localhost:$PORT"
echo "Directory: $SERVE_DIR"
echo "Press Ctrl+C to stop."

# Open browser (macOS / Linux)
if command -v open &>/dev/null; then
  open "http://localhost:$PORT" &
elif command -v xdg-open &>/dev/null; then
  xdg-open "http://localhost:$PORT" &
fi

cd "$SERVE_DIR"
python3 -m http.server "$PORT"
