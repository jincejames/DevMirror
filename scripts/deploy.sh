#!/usr/bin/env bash
#
# DevMirror App - Create, configure, and deploy to Databricks Apps.
#
# Usage:
#   ./scripts/deploy.sh                           # Deploy to existing app (default profile)
#   ./scripts/deploy.sh --profile my-profile      # Use a specific Databricks CLI profile
#   ./scripts/deploy.sh --create                  # Create a new app + deploy
#   ./scripts/deploy.sh --create --profile prod   # Create + deploy with a profile
#
# Prerequisites:
#   - Databricks CLI authenticated (databricks auth login)
#   - Node.js + npm (for frontend build)
#   - Python 3.11+
#
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
APP_NAME="devmirror"
APP_DESCRIPTION="DevMirror - UC dev environment cloning"
PROFILE=""
CREATE=false
WORKSPACE_PATH=""  # auto-derived from current user
WAREHOUSE_ID=""
CONTROL_CATALOG="users"
CONTROL_SCHEMA=""   # auto-derived from current user
ADMIN_GROUP="devmirror-admins"

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)       PROFILE="$2"; shift 2 ;;
    --create)        CREATE=true; shift ;;
    --app-name)      APP_NAME="$2"; shift 2 ;;
    --warehouse-id)  WAREHOUSE_ID="$2"; shift 2 ;;
    --catalog)       CONTROL_CATALOG="$2"; shift 2 ;;
    --schema)        CONTROL_SCHEMA="$2"; shift 2 ;;
    --admin-group)   ADMIN_GROUP="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--create] [--profile <name>] [--app-name <name>] [--warehouse-id <id>]"
      echo "          [--catalog <name>] [--schema <name>] [--admin-group <name>]"
      echo ""
      echo "Options:"
      echo "  --create         Create a new Databricks App (first-time setup)"
      echo "  --profile        Databricks CLI profile to use"
      echo "  --app-name       App name (default: devmirror)"
      echo "  --warehouse-id   SQL warehouse ID for Statement Execution API"
      echo "  --catalog        Control table catalog (default: users)"
      echo "  --schema         Control table schema (default: derived from user email)"
      echo "  --admin-group    Databricks group for admin role (default: devmirror-admins)"
      exit 0
      ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

PROFILE_FLAG=""
if [[ -n "$PROFILE" ]]; then
  PROFILE_FLAG="--profile $PROFILE"
fi

# ── Resolve current user ─────────────────────────────────────────────────────
echo "==> Resolving current user..."
USER_EMAIL=$(databricks current-user me $PROFILE_FLAG --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))" 2>/dev/null)
if [[ -z "$USER_EMAIL" ]]; then
  echo "ERROR: Could not determine current user. Is the Databricks CLI authenticated?"
  echo "  Run: databricks auth login --host <workspace-url> ${PROFILE:+--profile $PROFILE}"
  exit 1
fi
echo "    User: $USER_EMAIL"

# Auto-derive workspace path and schema from email
if [[ -z "$WORKSPACE_PATH" ]]; then
  WORKSPACE_PATH="/Workspace/Users/${USER_EMAIL}/devmirror-app"
fi
if [[ -z "$CONTROL_SCHEMA" ]]; then
  CONTROL_SCHEMA=$(echo "$USER_EMAIL" | sed 's/@.*//' | tr '.' '_')
fi

# ── Resolve warehouse ────────────────────────────────────────────────────────
if [[ -z "$WAREHOUSE_ID" ]]; then
  echo "==> Finding a running SQL warehouse..."
  WAREHOUSE_ID=$(databricks warehouses list $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "
import sys, json
whs = json.load(sys.stdin)
# Prefer Serverless Starter, then any RUNNING warehouse
for w in whs:
    if w.get('state') == 'RUNNING' and 'starter' in w.get('name','').lower():
        print(w['id']); exit()
for w in whs:
    if w.get('state') == 'RUNNING':
        print(w['id']); exit()
" 2>/dev/null)
  if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "ERROR: No running SQL warehouse found. Specify one with --warehouse-id <id>"
    exit 1
  fi
  echo "    Warehouse: $WAREHOUSE_ID"
fi

# ── Locate project root ──────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "==> Project root: $PROJECT_DIR"
echo "    App name:      $APP_NAME"
echo "    Workspace:     $WORKSPACE_PATH"
echo "    Warehouse:     $WAREHOUSE_ID"
echo "    Catalog:       $CONTROL_CATALOG"
echo "    Schema:        $CONTROL_SCHEMA"
echo "    Admin group:   $ADMIN_GROUP"
echo ""

# ── Create app (if --create) ─────────────────────────────────────────────────
if $CREATE; then
  echo "==> Creating Databricks App '$APP_NAME'..."
  databricks apps create "$APP_NAME" --description "$APP_DESCRIPTION" $PROFILE_FLAG 2>&1 || true

  echo "==> Waiting for app compute to start..."
  for i in $(seq 1 30); do
    STATE=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','?'))" 2>/dev/null)
    if [[ "$STATE" == "ACTIVE" ]]; then
      echo "    Compute is ACTIVE"
      break
    fi
    echo "    Compute state: $STATE (waiting...)"
    sleep 10
  done

  # Get the new service principal
  SP_CLIENT_ID=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))" 2>/dev/null)

  if [[ -n "$SP_CLIENT_ID" ]]; then
    echo "==> Granting service principal ($SP_CLIENT_ID) access to catalogs..."
    databricks grants update catalog "$CONTROL_CATALOG" \
      --json "{\"changes\": [{\"add\": [\"USE_CATALOG\"], \"principal\": \"$SP_CLIENT_ID\"}]}" \
      $PROFILE_FLAG > /dev/null 2>&1
    databricks grants update schema "${CONTROL_CATALOG}.${CONTROL_SCHEMA}" \
      --json "{\"changes\": [{\"add\": [\"ALL_PRIVILEGES\"], \"principal\": \"$SP_CLIENT_ID\"}]}" \
      $PROFILE_FLAG > /dev/null 2>&1
    echo "    Grants applied"
  fi
fi

# ── Build frontend ────────────────────────────────────────────────────────────
echo "==> Building frontend..."
cd "$PROJECT_DIR/app/ui"
npm run build --silent 2>&1
echo "    Frontend built"

# ── Write app.yaml with resolved values ───────────────────────────────────────
echo "==> Generating app.yaml..."
cat > "$PROJECT_DIR/app/app.yaml" <<YAML
command:
  - sh
  - -c
  - uvicorn backend.main:app --host=0.0.0.0 --port=\${DATABRICKS_APP_PORT:-8000}
env:
  - name: DEVMIRROR_WAREHOUSE_ID
    value: ${WAREHOUSE_ID}
  - name: DEVMIRROR_CONTROL_CATALOG
    value: ${CONTROL_CATALOG}
  - name: DEVMIRROR_CONTROL_SCHEMA
    value: ${CONTROL_SCHEMA}
  - name: DEVMIRROR_ADMIN_GROUP
    value: ${ADMIN_GROUP}
YAML
echo "    app.yaml written"

# ── Stage deployment package ──────────────────────────────────────────────────
echo "==> Staging deployment package..."
STAGING_DIR=$(mktemp -d)
cp -r "$PROJECT_DIR/app/backend" "$STAGING_DIR/backend"
cp -r "$PROJECT_DIR/devmirror" "$STAGING_DIR/devmirror"
cp "$PROJECT_DIR/app/app.yaml" "$STAGING_DIR/app.yaml"
cp "$PROJECT_DIR/app/pyproject.toml" "$STAGING_DIR/pyproject.toml"
cp "$PROJECT_DIR/app/requirements.txt" "$STAGING_DIR/requirements.txt"

# Clean pycache from staging
find "$STAGING_DIR" -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
echo "    Staged at $STAGING_DIR"

# ── Upload to workspace ──────────────────────────────────────────────────────
echo "==> Uploading to $WORKSPACE_PATH..."
databricks workspace import-dir "$STAGING_DIR" "$WORKSPACE_PATH" \
  $PROFILE_FLAG --overwrite 2>&1 | tail -1
echo "    Upload complete"

# ── Deploy ────────────────────────────────────────────────────────────────────
echo "==> Deploying app '$APP_NAME'..."
DEPLOY_OUTPUT=$(databricks apps deploy "$APP_NAME" \
  --source-code-path "$WORKSPACE_PATH" \
  $PROFILE_FLAG 2>&1)

DEPLOY_STATE=$(echo "$DEPLOY_OUTPUT" | python3 -c "import sys,json; print(json.load(sys.stdin).get('status',{}).get('state','?'))" 2>/dev/null)
echo "$DEPLOY_OUTPUT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f\"    Status: {d['status']['state']}\")" 2>/dev/null

# ── Get app URL ───────────────────────────────────────────────────────────────
APP_URL=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('url',''))" 2>/dev/null)

# ── Cleanup ───────────────────────────────────────────────────────────────────
rm -rf "$STAGING_DIR"

echo ""
echo "=========================================="
echo "  DevMirror deployed successfully!"
echo "  URL: $APP_URL"
echo "=========================================="
