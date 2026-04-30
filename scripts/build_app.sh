#!/usr/bin/env bash
#
# Stage the Databricks App source tree at ./app/ for DAB-managed deploys.
#
# DevMirror has two co-equal source dirs (./app and ./devmirror).  The
# Databricks Apps platform expects a single source_code_path containing
# the FastAPI backend, the built frontend, and any Python packages the
# backend imports.  This script:
#
#   1. Builds the React frontend into app/backend/static/.
#   2. Copies the devmirror engine package into app/devmirror/ so the
#      backend's `from devmirror.x import y` imports resolve at runtime.
#
# After running this, ./app/ is the canonical source_code_path for both
# `databricks bundle deploy` and `databricks apps deploy`.
#
# Usage:
#   ./scripts/build_app.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Building frontend..."
cd "$PROJECT_DIR/app/ui"
npm run build --silent
echo "    Frontend built into app/backend/static/"

echo "==> Staging devmirror engine into app/devmirror/..."
rm -rf "$PROJECT_DIR/app/devmirror"
# rsync to keep ownership clean and to avoid copying __pycache__ trees.
if command -v rsync >/dev/null 2>&1; then
  rsync -a \
    --exclude '__pycache__' \
    --exclude '*.pyc' \
    "$PROJECT_DIR/devmirror/" "$PROJECT_DIR/app/devmirror/"
else
  cp -r "$PROJECT_DIR/devmirror" "$PROJECT_DIR/app/devmirror"
  find "$PROJECT_DIR/app/devmirror" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
  find "$PROJECT_DIR/app/devmirror" -name '*.pyc' -delete 2>/dev/null || true
fi
echo "    devmirror staged at app/devmirror/"

echo ""
echo "App source tree ready at: $PROJECT_DIR/app"
echo "Next: databricks bundle deploy --target dev --profile <name> --var ..."
