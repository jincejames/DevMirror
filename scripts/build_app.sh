#!/usr/bin/env bash
#
# Stage the Databricks App source tree at ./app/ for DAB-managed deploys.
#
# DevMirror's FastAPI backend (./app/backend/) imports the engine
# package as ``devmirror.*``.  At runtime the app process starts
# ``uvicorn backend.main:app`` from ``./app/``, so ``./app/`` ends up at
# the front of ``sys.path``.  We make ``devmirror`` importable by
# building a wheel from the engine source at ``./devmirror/`` and
# dropping it into ``./app/dist/`` -- ``app/requirements.txt`` then
# pip-installs that wheel at deploy time.  This is the canonical
# source-of-truth for the engine; there is intentionally no
# ``./app/devmirror/`` source mirror (the historical staged copy
# silently shadowed the wheel and drifted between deploys).
#
# Steps:
#   1. Builds the React frontend into ``app/backend/static/``.
#   2. Builds the devmirror wheel into ``app/dist/`` so the runtime
#      pip install picks it up.
#
# Usage:
#   ./scripts/build_app.sh
#
# Note: ``databricks bundle deploy`` (via the customer bundle's
# ``artifacts.devmirror_wheel`` entry) also produces a wheel for
# the lifecycle job under ``customers/<id>/dist/``.  Running this
# script locally is only required for non-bundle deploys (e.g.
# ``databricks apps deploy``) or to refresh ``app/dist/`` between
# bundle invocations during development.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Building frontend..."
cd "$PROJECT_DIR/app/ui"
npm run build --silent
echo "    Frontend built into app/backend/static/"

echo "==> Building devmirror wheel into app/dist/..."
rm -rf "$PROJECT_DIR/app/dist"
mkdir -p "$PROJECT_DIR/app/dist"
# --no-deps: requirements.txt pins the runtime deps explicitly; the wheel
# itself only needs the engine code, no transitive resolution required.
pip wheel --no-deps --wheel-dir "$PROJECT_DIR/app/dist" "$PROJECT_DIR" >/dev/null
echo "    Built: $(ls "$PROJECT_DIR/app/dist/")"

echo ""
echo "App source tree ready at: $PROJECT_DIR/app"
echo "Next: databricks bundle deploy --target <target> --profile <name>"
