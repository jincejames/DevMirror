#!/usr/bin/env bash
#
# DevMirror -- thin convenience wrapper around the Databricks Asset Bundle.
#
# Canonical deploy path:
#
#   ./scripts/build_app.sh                                          # 1. stage app source
#   databricks bundle deploy --target dev --profile <name> --var ...# 2. upload + register
#   databricks bundle run devmirror --target dev --profile <name>   # 3. trigger apps deploy
#
# This script wraps all three steps with sensible defaults (auto-pick a
# running warehouse, derive the control schema from your user email).  For
# first-time setup in a new workspace, run scripts/bootstrap_env.sh first
# to create the app and grant its service principal catalog/schema access.
#
# Usage:
#   ./scripts/deploy.sh                                # dev target, default profile
#   ./scripts/deploy.sh --profile prod --target prod \
#       --warehouse-id <id> --catalog dev_analytics \
#       --schema devmirror_admin --run-as-sp <app-id>
#
set -euo pipefail

PROFILE=""
TARGET="dev"
APP_NAME="devmirror"
WAREHOUSE_ID=""
CONTROL_CATALOG="dev_analytics"
CONTROL_SCHEMA=""
ADMIN_GROUP="devmirror-admins"
RUN_AS_SP=""
NODE_TYPE_ID="i3.xlarge"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)        PROFILE="$2";        shift 2 ;;
    --target)         TARGET="$2";         shift 2 ;;
    --app-name)       APP_NAME="$2";       shift 2 ;;
    --warehouse-id)   WAREHOUSE_ID="$2";   shift 2 ;;
    --catalog)        CONTROL_CATALOG="$2"; shift 2 ;;
    --schema)         CONTROL_SCHEMA="$2"; shift 2 ;;
    --admin-group)    ADMIN_GROUP="$2";    shift 2 ;;
    --run-as-sp)      RUN_AS_SP="$2";      shift 2 ;;
    --node-type-id)   NODE_TYPE_ID="$2";   shift 2 ;;
    -h|--help)
      cat <<HELP
Wrapper around 'databricks bundle deploy && databricks bundle run' for
the DevMirror app + lifecycle DAB.

Options:
  --profile         Databricks CLI profile (required for non-default profile)
  --target          Bundle target: dev (default) or prod
  --app-name        Override the app resource name (default: devmirror)
  --warehouse-id    SQL warehouse ID (auto-discovered if omitted)
  --catalog         Control table catalog (default: dev_analytics)
  --schema          Control table schema (default: <user>_admin from email)
  --admin-group     Account group name for admin role (default: devmirror-admins)
  --run-as-sp       Service principal application_id for prod target
  --node-type-id    Cluster node type for the lifecycle job (default: i3.xlarge)

For first-time setup in a new workspace, run scripts/bootstrap_env.sh first.
For SMTP/Teams notifications, see databricks.yml comments.
HELP
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

PROFILE_FLAG=""
[[ -n "$PROFILE" ]] && PROFILE_FLAG="--profile $PROFILE"

# ── Auto-resolve missing inputs ───────────────────────────────────────────────
if [[ -z "$CONTROL_SCHEMA" ]]; then
  USER_EMAIL=$(databricks current-user me $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))")
  CONTROL_SCHEMA=$(echo "$USER_EMAIL" | sed 's/@.*//' | tr '.' '_')
  echo "==> Derived control schema from user: $CONTROL_SCHEMA"
fi

if [[ -z "$WAREHOUSE_ID" ]]; then
  echo "==> Auto-picking a running SQL warehouse..."
  WAREHOUSE_ID=$(databricks warehouses list $PROFILE_FLAG --output json 2>/dev/null | python3 -c "
import sys, json
whs = json.load(sys.stdin)
for w in whs:
    if w.get('state') == 'RUNNING' and 'starter' in w.get('name','').lower():
        print(w['id']); exit()
for w in whs:
    if w.get('state') == 'RUNNING':
        print(w['id']); exit()
")
  if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "ERROR: No running SQL warehouse. Pass --warehouse-id <id>." >&2
    exit 1
  fi
  echo "    Warehouse: $WAREHOUSE_ID"
fi

# ── Build app source tree ─────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
"$SCRIPT_DIR/build_app.sh"

# Bundle vars used by both `deploy` and `run` (run needs the same set
# because each invocation re-resolves the bundle config).
BUNDLE_VARS=(
  --var "warehouse_id=$WAREHOUSE_ID"
  --var "node_type_id=$NODE_TYPE_ID"
  --var "app_name=$APP_NAME"
  --var "control_catalog=$CONTROL_CATALOG"
  --var "control_schema=$CONTROL_SCHEMA"
  --var "admin_group=$ADMIN_GROUP"
)
[[ -n "$RUN_AS_SP" ]] && BUNDLE_VARS+=(--var "run_as_sp=$RUN_AS_SP")

# ── Bundle deploy ─────────────────────────────────────────────────────────────
echo "==> databricks bundle deploy --target $TARGET ..."
databricks bundle deploy --target "$TARGET" $PROFILE_FLAG "${BUNDLE_VARS[@]}"

# ── Trigger apps deploy ───────────────────────────────────────────────────────
echo "==> databricks bundle run $APP_NAME ..."
databricks bundle run "$APP_NAME" --target "$TARGET" $PROFILE_FLAG "${BUNDLE_VARS[@]}"

# ── Report URL ────────────────────────────────────────────────────────────────
APP_URL=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))")

echo ""
echo "=========================================="
echo "  DevMirror deployed"
echo "  Target:   $TARGET"
echo "  App URL:  $APP_URL"
echo "=========================================="
