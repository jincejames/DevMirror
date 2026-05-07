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
#   5. Applies the control-table DDL (devmirror_development_requests,
#      devmirror_dr_objects, devmirror_dr_access, audit_log,
#      devmirror_configs, devmirror_id_counter) via the Statement
#      Execution API.  Idempotent (every CREATE TABLE uses IF NOT EXISTS).
#
# Run ONCE per workspace, before the first `./scripts/deploy.sh`.
#
# Usage:
#   ./scripts/bootstrap_env.sh --profile <name> [--app-name <n>] \
#       [--catalog <cat>] [--schema <sch>] [--warehouse-id <id>] \
#       [--smtp-password-secret <key>=<scope>/<key>] \
#       [--teams-webhook-secret <key>=<scope>/<key>]
#
# Examples:
#   ./scripts/bootstrap_env.sh --profile prod \
#       --catalog dev_analytics --schema devmirror_admin \
#       --warehouse-id e9b34f7a2e4b0561 \
#       --smtp-password-secret smtp-pw=devmirror/smtp-password \
#       --teams-webhook-secret teams=devmirror/teams-webhook-url
#
# If --warehouse-id is omitted, the script picks the first RUNNING SQL
# warehouse visible to the profile.  Fails if none are running.
#
set -euo pipefail

PROFILE=""
APP_NAME="devmirror"
APP_DESCRIPTION="DevMirror -- UC dev environment cloning"
CONTROL_CATALOG="dev_analytics"
CONTROL_SCHEMA=""
WAREHOUSE_ID=""
SMTP_PASSWORD_SECRET=""
TEAMS_WEBHOOK_SECRET=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --profile)                PROFILE="$2";              shift 2 ;;
    --app-name)               APP_NAME="$2";             shift 2 ;;
    --catalog)                CONTROL_CATALOG="$2";      shift 2 ;;
    --schema)                 CONTROL_SCHEMA="$2";       shift 2 ;;
    --warehouse-id)           WAREHOUSE_ID="$2";         shift 2 ;;
    --smtp-password-secret)   SMTP_PASSWORD_SECRET="$2"; shift 2 ;;
    --teams-webhook-secret)   TEAMS_WEBHOOK_SECRET="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,35p' "$0"; exit 0 ;;
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

# Auto-pick a RUNNING SQL warehouse for the DDL step (step 5) if --warehouse-id wasn't passed.
if [[ -z "$WAREHOUSE_ID" ]]; then
  WAREHOUSE_ID=$(databricks warehouses list $PROFILE_FLAG --output json 2>/dev/null \
    | python3 -c "
import sys, json
for w in json.load(sys.stdin) or []:
    if w.get('state') == 'RUNNING':
        print(w.get('id', ''))
        break
")
  if [[ -z "$WAREHOUSE_ID" ]]; then
    echo "ERROR: --warehouse-id not provided and no RUNNING SQL warehouse found." >&2
    echo "       Start a warehouse and re-run, or pass --warehouse-id <id>." >&2
    exit 1
  fi
  echo "==> Auto-selected RUNNING warehouse: $WAREHOUSE_ID"
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

# ── 5. Apply control-table DDL (idempotent: CREATE TABLE IF NOT EXISTS) ──────
# Resolve the workspace host -- the CLI doesn't expose it via `me`, so read
# it from ~/.databrickscfg for the named profile.  Falls back to the
# DATABRICKS_HOST env var if no profile is set.
HOST="${DATABRICKS_HOST:-}"
if [[ -z "$HOST" && -n "$PROFILE" && -f "$HOME/.databrickscfg" ]]; then
  HOST=$(awk -v p="[$PROFILE]" '
    $0==p {found=1; next}
    found && /^host[[:space:]]*=/ {sub(/^host[[:space:]]*=[[:space:]]*/,""); print; exit}
    found && /^\[/ {exit}
  ' "$HOME/.databrickscfg")
fi
if [[ -z "$HOST" ]]; then
  echo "ERROR: cannot resolve workspace host for profile '$PROFILE' --" >&2
  echo "       set DATABRICKS_HOST or add a host entry under [$PROFILE] in ~/.databrickscfg" >&2
  exit 1
fi

TOKEN=$(databricks auth token $PROFILE_FLAG \
  | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")

echo "==> Applying control-table DDL on $CONTROL_CATALOG.$CONTROL_SCHEMA via warehouse $WAREHOUSE_ID..."
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
python3 - "$PROJECT_DIR/devmirror/migrations" "$CONTROL_CATALOG" "$CONTROL_SCHEMA" \
        "$HOST" "$TOKEN" "$WAREHOUSE_ID" <<'PY'
import json
import pathlib
import sys
import urllib.request

mig_dir, ctrl_cat, ctrl_sch, host, token, wh = sys.argv[1:7]
applied = 0
failed = 0

for f in sorted(pathlib.Path(mig_dir).glob("*.sql")):
    raw = f.read_text()
    rendered = (
        raw.replace("{control_catalog}", ctrl_cat)
        .replace("{control_schema}", ctrl_sch)
    )
    current: list[str] = []
    for line in rendered.splitlines():
        s = line.strip()
        if not current and (s.startswith("--") or not s):
            continue
        if s.startswith("--"):
            continue
        current.append(line)
        if s.endswith(";"):
            stmt = "\n".join(current).strip().rstrip(";")
            current = []
            if not stmt:
                continue
            body = json.dumps({
                "statement": stmt,
                "warehouse_id": wh,
                "wait_timeout": "30s",
            }).encode()
            req = urllib.request.Request(
                f"{host.rstrip('/')}/api/2.0/sql/statements",
                data=body, method="POST",
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
            )
            try:
                resp = json.loads(urllib.request.urlopen(req).read())
            except Exception as e:
                failed += 1
                preview = stmt.split("\n")[0][:70]
                print(f"    [ERR] {f.name}: {preview} -- {e}")
                continue
            state = resp.get("status", {}).get("state")
            preview = stmt.split("\n")[0][:70].replace("    ", " ")
            if state == "SUCCEEDED":
                applied += 1
                print(f"    [OK] {f.name}: {preview}")
            else:
                failed += 1
                err = resp.get("status", {}).get("error", {})
                print(f"    [FAIL] {f.name}: {preview} -- {err}")

print(f"==> DDL applied: {applied} statement(s), {failed} failure(s)")
sys.exit(1 if failed else 0)
PY

echo ""
echo "=========================================="
echo "  Bootstrap complete for app '$APP_NAME'"
echo "  Service principal: $SP_CLIENT_ID"
echo "  Control tables   : $CONTROL_CATALOG.$CONTROL_SCHEMA (DDL applied)"
echo ""
echo "  Next: ./scripts/deploy.sh --profile $PROFILE"
echo "=========================================="
