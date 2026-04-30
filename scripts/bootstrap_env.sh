#!/usr/bin/env bash
#
# DevMirror -- one-time setup for a new workspace.
#
# What this script does (idempotent where possible):
#   1. Creates the Databricks App resource (`apps create`).
#   2. Waits for the app's compute to become ACTIVE.
#   3. Grants the app's auto-created service principal:
#        - USE_CATALOG on the control catalog
#        - ALL_PRIVILEGES on the control schema
#   4. (Optional) Pre-creates Databricks Apps Secret resources for SMTP /
#      Teams notification credentials.  After this, the bundle's
#      `apps.devmirror.config.env` can reference them via `valueFrom`.
#
# Run ONCE per workspace, before the first `./scripts/deploy.sh`.
#
# Usage:
#   ./scripts/bootstrap_env.sh --profile <name> [--app-name <n>] \
#       [--catalog <cat>] [--schema <sch>] \
#       [--smtp-password-secret <key>=<scope>/<key>] \
#       [--teams-webhook-secret <key>=<scope>/<key>]
#
# Examples:
#   ./scripts/bootstrap_env.sh --profile prod \
#       --catalog dev_analytics --schema devmirror_admin \
#       --smtp-password-secret smtp-pw=devmirror/smtp-password \
#       --teams-webhook-secret teams=devmirror/teams-webhook-url
#
set -euo pipefail

PROFILE=""
APP_NAME="devmirror"
APP_DESCRIPTION="DevMirror -- UC dev environment cloning"
CONTROL_CATALOG="dev_analytics"
CONTROL_SCHEMA=""
SMTP_PASSWORD_SECRET=""
TEAMS_WEBHOOK_SECRET=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)                PROFILE="$2";              shift 2 ;;
    --app-name)               APP_NAME="$2";             shift 2 ;;
    --catalog)                CONTROL_CATALOG="$2";      shift 2 ;;
    --schema)                 CONTROL_SCHEMA="$2";       shift 2 ;;
    --smtp-password-secret)   SMTP_PASSWORD_SECRET="$2"; shift 2 ;;
    --teams-webhook-secret)   TEAMS_WEBHOOK_SECRET="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,30p' "$0"; exit 0 ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

PROFILE_FLAG=""
[[ -n "$PROFILE" ]] && PROFILE_FLAG="--profile $PROFILE"

# Derive control schema from email if not supplied.
if [[ -z "$CONTROL_SCHEMA" ]]; then
  USER_EMAIL=$(databricks current-user me $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('userName',''))")
  CONTROL_SCHEMA=$(echo "$USER_EMAIL" | sed 's/@.*//' | tr '.' '_')
  echo "==> Derived control schema from user: $CONTROL_SCHEMA"
fi

# ── 1. Create app (idempotent: ignore "already exists") ──────────────────────
echo "==> Creating Databricks App '$APP_NAME'..."
databricks apps create "$APP_NAME" --description "$APP_DESCRIPTION" $PROFILE_FLAG 2>&1 || true

# ── 2. Wait for compute to start ─────────────────────────────────────────────
echo "==> Waiting for app compute to become ACTIVE..."
for i in $(seq 1 30); do
  STATE=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "import sys,json; print(json.load(sys.stdin).get('compute_status',{}).get('state','?'))")
  if [[ "$STATE" == "ACTIVE" ]]; then
    echo "    ACTIVE"
    break
  fi
  echo "    state=$STATE (waiting...)"
  sleep 10
done

# ── 3. Grant the app's SP catalog / schema access ────────────────────────────
SP_CLIENT_ID=$(databricks apps get "$APP_NAME" $PROFILE_FLAG --output json 2>/dev/null \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('service_principal_client_id',''))")

if [[ -n "$SP_CLIENT_ID" ]]; then
  echo "==> Granting app SP ($SP_CLIENT_ID) access to control plane..."
  databricks grants update catalog "$CONTROL_CATALOG" \
    --json "{\"changes\": [{\"add\": [\"USE_CATALOG\"], \"principal\": \"$SP_CLIENT_ID\"}]}" \
    $PROFILE_FLAG > /dev/null
  databricks grants update schema "${CONTROL_CATALOG}.${CONTROL_SCHEMA}" \
    --json "{\"changes\": [{\"add\": [\"ALL_PRIVILEGES\"], \"principal\": \"$SP_CLIENT_ID\"}]}" \
    $PROFILE_FLAG > /dev/null
  echo "    Grants applied"
else
  echo "    WARN: could not resolve app service principal -- skipping grants"
fi

# ── 4. Pre-create Secret app resources (optional) ─────────────────────────────
_create_secret_resource() {
  # Args: <resource-key>=<scope>/<key>
  local arg="$1" key scope sec
  key="${arg%%=*}"
  local rest="${arg#*=}"
  scope="${rest%%/*}"
  sec="${rest#*/}"
  echo "==> Adding Secret resource '$key' (scope=$scope, key=$sec) to app..."
  databricks apps update "$APP_NAME" --json "$(cat <<JSON
{
  "resources": [
    {
      "name": "$key",
      "secret": {"scope": "$scope", "key": "$sec", "permission": "READ"}
    }
  ]
}
JSON
)" $PROFILE_FLAG > /dev/null
  echo "    Resource '$key' attached"
}

if [[ -n "$SMTP_PASSWORD_SECRET" ]]; then
  _create_secret_resource "$SMTP_PASSWORD_SECRET"
fi
if [[ -n "$TEAMS_WEBHOOK_SECRET" ]]; then
  _create_secret_resource "$TEAMS_WEBHOOK_SECRET"
fi

echo ""
echo "=========================================="
echo "  Bootstrap complete for app '$APP_NAME'"
echo "  Service principal: $SP_CLIENT_ID"
echo ""
echo "  Next: ./scripts/deploy.sh --profile $PROFILE"
echo "=========================================="
