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

# ─── SP-2: copy README + docs/images into the served dir for the landing ────
# The landing page (index.html) fetches /README.md and renders it via
# marked.js. The README and its images live at the repo root, but the
# static server only sees the served directory. Copy them in on every
# boot. Both copies are gitignored.
copy_landing_assets() {
  local served_dir="$1"
  # Find the project root (the dir containing pom.xml).
  local project_root="$SCRIPT_DIR"
  while [ "$project_root" != "/" ] && [ ! -f "$project_root/pom.xml" ]; do
    project_root="$(dirname "$project_root")"
  done
  if [ ! -f "$project_root/pom.xml" ]; then
    echo "WARN: could not find pom.xml above $SCRIPT_DIR; landing README + images may be missing" >&2
    return 0
  fi
  if [ -f "$project_root/README.md" ]; then
    cp "$project_root/README.md" "$served_dir/README.md"
  fi
  if [ -f "$project_root/ROADMAP.md" ]; then
    cp "$project_root/ROADMAP.md" "$served_dir/ROADMAP.md"
  fi
  if [ -d "$project_root/docs/images" ]; then
    mkdir -p "$served_dir/docs/images"
    cp -R "$project_root/docs/images/." "$served_dir/docs/images/"
  fi
}

# ─── --api mode: launch TunerService ────────────────────────────────────────
if [ "${1:-}" = "--api" ]; then
  shift
  force_rebuild=0
  # Pass --rebuild (or -rebuild) anywhere to force `mvn package` even when
  # the slim jar is current.
  #
  # macOS Bash 3.2 + `set -u` doesn't tolerate `"${empty_array[@]}"`, even via
  # the `${arr[@]+...}` idiom on some patch levels. Use a length check + a
  # plain string buffer to stay portable. (We don't expect args containing
  # whitespace here — every flag is `--key=value`.)
  rebuild_filtered_args=""
  for a in "$@"; do
    case "$a" in
      --rebuild|-rebuild) force_rebuild=1 ;;
      *) rebuild_filtered_args="$rebuild_filtered_args $a" ;;
    esac
  done
  # shellcheck disable=SC2086
  set -- $rebuild_filtered_args

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
  OPEN_URL="http://127.0.0.1:$PORT/dashboard.html"

  echo "Booting TunerService on $OPEN_URL"
  echo "  Project root: $PROJECT_ROOT"
  echo "  Frontend dir: $SCRIPT_DIR"

  # Rebuild when:
  #   - the slim jar is missing
  #   - target/lib/ is missing or empty
  #   - any source under com/db/serna/orchestration is newer than the jar
  #   - pom.xml is newer than the jar
  #   - --rebuild was passed
  needs_build=$force_rebuild
  if [ ! -f "$JAR" ]; then needs_build=1; reason="jar missing"; fi
  if [ "$needs_build" = "0" ] && { [ ! -d "$LIB" ] || [ -z "$(ls -A "$LIB" 2>/dev/null)" ]; }; then
    needs_build=1; reason="target/lib/ missing or empty"
  fi
  if [ "$needs_build" = "0" ] && [ -f "$JAR" ]; then
    if [ "$PROJECT_ROOT/pom.xml" -nt "$JAR" ]; then
      needs_build=1; reason="pom.xml is newer than jar"
    elif [ -n "$(find "$PROJECT_ROOT/src/main/scala/com/db/serna/orchestration" -name '*.scala' -newer "$JAR" -print -quit 2>/dev/null)" ]; then
      needs_build=1; reason="Scala sources are newer than jar"
    fi
  fi
  if [ "$force_rebuild" = "1" ]; then needs_build=1; reason="--rebuild flag"; fi

  if [ "$needs_build" = "1" ]; then
    if ! command -v mvn &>/dev/null; then
      echo "Error: rebuild required ($reason) but Maven is not on PATH." >&2
      echo "Install Maven, or build manually with: mvn -Pserve package" >&2
      exit 1
    fi
    echo "  Rebuilding ($reason) — mvn -Pserve package -DskipTests…"
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

  copy_landing_assets "$SCRIPT_DIR"

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

  cp -n "$SCRIPT_DIR/dashboard.html"  "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/style.css"   "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/app.js"      "$OUTPUT_DIR/" 2>/dev/null || true
  cp -n "$SCRIPT_DIR/config.json" "$OUTPUT_DIR/" 2>/dev/null || true

  SERVE_DIR="$PARENT_DIR"
  OPEN_URL="http://localhost:$PORT/$CUR_NAME/dashboard.html?data=_auto_tuner_analysis.json"

  echo "Serving auto-tuner dashboard at $OPEN_URL"
  echo "Directory: $SERVE_DIR"
  echo "Press Ctrl+C to stop."

  copy_landing_assets "$SCRIPT_DIR"

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
if [ -z "$REL_FRONTEND" ] || [ "$REL_FRONTEND" = "." ]; then
  OPEN_URL="http://localhost:$PORT/"
else
  OPEN_URL="http://localhost:$PORT/$REL_FRONTEND/"
fi

echo "Serving OSS landing page (renders README.md). Click 'Skip to dashboard →' for the auto-tuner UI."
echo "  Serving root: $SERVE_DIR"
echo "  Frontend URL: $OPEN_URL"
echo "  Outputs URL:  http://localhost:$PORT/$REL_OUTPUTS/"
echo "  Outputs dir:  $OUTPUTS_DIR"
echo "Press Ctrl+C to stop."

copy_landing_assets "$SCRIPT_DIR"

if command -v open &>/dev/null;      then open "$OPEN_URL" &
elif command -v xdg-open &>/dev/null; then xdg-open "$OPEN_URL" &
fi

cd "$SERVE_DIR"
exec python3 -m http.server "$PORT"
