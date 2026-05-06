#!/usr/bin/env bash
#
# Serves the auto-tuner dashboard on localhost:8080.
#
# Usage:
#   ./serve.sh
#       Static-only (Phase 1). Reads ./config.json, resolves outputsPath, and
#       serves from the smallest directory that contains both this frontend
#       dir and the outputs dir. Opens the landing page.
#
#   ./serve.sh --api
#       API mode (Phase 2). Boots the Scala TunerService which serves the
#       static files AND exposes /api/* endpoints for the wizard's "Start
#       Tuning" button. Tries `java -jar target/spark-cluster-job-tuner-server.jar`
#       first; falls back to `mvn -Pserve exec:java` if the JAR isn't built yet.
#
#   ./serve.sh /path/to/outputs/<curDate>_auto_tuned
#       Back-compat: serves from the PARENT of that dir and opens
#       ?data=_auto_tuner_analysis.json, skipping the landing.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT="${PORT:-8080}"

# ─── --api mode: launch TunerService ────────────────────────────────────────
if [ "${1:-}" = "--api" ]; then
  shift
  # Walk up to find the project root (the dir that contains pom.xml).
  PROJECT_ROOT="$SCRIPT_DIR"
  while [ "$PROJECT_ROOT" != "/" ] && [ ! -f "$PROJECT_ROOT/pom.xml" ]; do
    PROJECT_ROOT="$(dirname "$PROJECT_ROOT")"
  done
  if [ ! -f "$PROJECT_ROOT/pom.xml" ]; then
    echo "Error: could not find project root (pom.xml) above $SCRIPT_DIR" >&2
    exit 1
  fi

  JAR="$PROJECT_ROOT/target/spark-cluster-job-tuner-server.jar"
  LIB="$PROJECT_ROOT/target/lib"
  OPEN_URL="http://127.0.0.1:$PORT/"

  echo "Booting TunerService on $OPEN_URL"
  echo "  Project root: $PROJECT_ROOT"
  echo "  Frontend dir: $SCRIPT_DIR"

  # First boot — or anyone who blew away target/ — will be missing the slim
  # jar and/or its sibling lib/ folder. Build them automatically rather than
  # asking the user to run a separate command.
  needs_build=0
  if [ ! -f "$JAR" ]; then needs_build=1; fi
  if [ ! -d "$LIB" ] || [ -z "$(ls -A "$LIB" 2>/dev/null)" ]; then needs_build=1; fi
  if [ "$needs_build" = "1" ]; then
    if ! command -v mvn &>/dev/null; then
      echo "Error: target/spark-cluster-job-tuner-server.jar (or target/lib/) is missing and Maven is not on PATH." >&2
      echo "Install Maven, or build manually with: mvn -Pserve package" >&2
      exit 1
    fi
    echo "  First boot — building project (mvn -Pserve package)…"
    cd "$PROJECT_ROOT"
    if ! mvn -q -Pserve package -DskipTests; then
      echo "Error: mvn -Pserve package failed." >&2
      echo "Re-run with 'mvn -Pserve package -e' from $PROJECT_ROOT to see the full stack trace." >&2
      exit 1
    fi
    if [ ! -f "$JAR" ] || [ ! -d "$LIB" ]; then
      echo "Error: build succeeded but $JAR or $LIB is still missing — check pom.xml's serve profile." >&2
      exit 1
    fi
    echo "  Build complete."
  fi

  if command -v open &>/dev/null;       then ( sleep 1.2 && open "$OPEN_URL" ) &
  elif command -v xdg-open &>/dev/null; then ( sleep 1.2 && xdg-open "$OPEN_URL" ) &
  fi

  cd "$PROJECT_ROOT"
  echo "  Mode: java -jar $(basename "$JAR")"
  exec java -jar "$JAR" \
    --port="$PORT" --host=127.0.0.1 \
    --frontend-dir="$SCRIPT_DIR" \
    "$@"
fi

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
  cat <<EOF
Error: config.json not found next to serve.sh

  Expected at: $CONFIG_FILE

Create one with the following keys (paths relative to config.json):
{
  "gcpProjectId": "your-gcp-project-id",
  "inputsPath":  "../src/main/resources/composer/dwh/config/cluster_tuning/inputs",
  "outputsPath": "../src/main/resources/composer/dwh/config/cluster_tuning/outputs"
}
EOF
  exit 1
fi

# Let python handle path math — bash's common-ancestor logic is fragile.
RESOLVED="$(
  python3 - "$CONFIG_FILE" "$SCRIPT_DIR" <<'PY'
import json, os, sys

cfg_path, script_dir = sys.argv[1], sys.argv[2]

with open(cfg_path) as f:
    cfg = json.load(f)

outputs_rel = cfg.get("outputsPath")
if not outputs_rel:
    print("ERROR: outputsPath missing from config.json", file=sys.stderr)
    sys.exit(2)

cfg_dir = os.path.dirname(os.path.abspath(cfg_path))
outputs_abs = os.path.realpath(os.path.join(cfg_dir, outputs_rel))
script_abs  = os.path.realpath(script_dir)

if not os.path.isdir(outputs_abs):
    print(f"ERROR: outputsPath does not exist: {outputs_abs}", file=sys.stderr)
    sys.exit(3)

# Serve from the smallest dir that contains both.
try:
    common = os.path.commonpath([outputs_abs, script_abs])
except ValueError:
    print("ERROR: frontend dir and outputsPath have no common ancestor", file=sys.stderr)
    sys.exit(4)

rel_frontend = os.path.relpath(script_abs, common)
rel_outputs  = os.path.relpath(outputs_abs, common)

# Output: tab-separated so bash can `read` it cleanly.
print(f"{common}\t{rel_frontend}\t{rel_outputs}\t{outputs_abs}")
PY
)" || { echo "Error resolving config paths (see above)"; exit 1; }

IFS=$'\t' read -r SERVE_DIR REL_FRONTEND REL_OUTPUTS OUTPUTS_DIR <<<"$RESOLVED"

# On the off-chance REL_FRONTEND == ".", we're already at the web root.
if [ "$REL_FRONTEND" = "." ]; then
  OPEN_URL="http://localhost:$PORT/"
else
  OPEN_URL="http://localhost:$PORT/$REL_FRONTEND/"
fi

echo "Serving auto-tuner landing page"
echo "  Serving root: $SERVE_DIR"
echo "  Frontend URL: $OPEN_URL"
echo "  Outputs URL:  http://localhost:$PORT/$REL_OUTPUTS/"
echo "  Outputs dir:  $OUTPUTS_DIR"
echo "Press Ctrl+C to stop."

if command -v open &>/dev/null;      then open "$OPEN_URL" &
elif command -v xdg-open &>/dev/null; then xdg-open "$OPEN_URL" &
fi

cd "$SERVE_DIR"
exec python3 -m http.server "$PORT"
