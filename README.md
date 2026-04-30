# DevMirror

Production-to-Development Unity Catalog object cloning utility for Databricks.

DevMirror automates the cloning and lifecycle management of production Unity Catalog objects (schemas, tables, views) into isolated development environments. Developers can test against real production data without interfering with other developers or production workloads.

## Documentation

- [SPECIFICATION.md](SPECIFICATION.md) -- full feature specification (v1.0)
- [Feature Spec](lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md) -- requirements and acceptance scenarios
- [Data Model](lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md) -- entity definitions and state transitions
- [CLI Contracts](lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/cli-commands.md) -- CLI command reference
- [Quickstart](lakeforge-specs/001-devmirror-dr-cloning-lifecycle/quickstart.md) -- getting started guide

## Installation

```bash
pip install -e .

# With development dependencies
pip install -e ".[dev]"
```

Requires Python 3.11+.

## Environment Variables

DevMirror reads configuration from environment variables:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DEVMIRROR_WAREHOUSE_ID` | Yes | -- | Databricks SQL warehouse ID for statement execution |
| `DEVMIRROR_CONTROL_CATALOG` | No | `dev_analytics` | Catalog for DevMirror control/metadata tables |
| `DEVMIRROR_CONTROL_SCHEMA` | No | `devmirror_admin` | Schema for DevMirror control/metadata tables |
| `DATABRICKS_CONFIG_PROFILE` | No | -- | Databricks SDK authentication profile |
| `DEVMIRROR_MAX_DR_DURATION_DAYS` | No | `90` | Maximum allowed DR lifetime in days |
| `DEVMIRROR_DEFAULT_NOTIFICATION_DAYS` | No | `7` | Days before expiry to send notification |
| `DEVMIRROR_SHALLOW_CLONE_THRESHOLD_GB` | No | `50` | Size threshold (GB) for shallow clone |
| `DEVMIRROR_MAX_PARALLEL_CLONES` | No | `10` | Max concurrent clone operations |
| `DEVMIRROR_AUDIT_RETENTION_DAYS` | No | `365` | Audit log retention in days |
| `DEVMIRROR_LINEAGE_SYSTEM_TABLE` | No | `system.access.table_lineage` | Lineage table FQN |
| `DEVMIRROR_DR_ID_PREFIX` | No | `DR` | DR ID prefix. Must match `^[A-Za-z][A-Za-z0-9]{0,7}$` |
| `DEVMIRROR_DR_ID_PADDING` | No | `5` | Zero-padding width for the DR counter. Integer, `3 <= N <= 12` |

Authentication to Databricks is handled by the `databricks-sdk` auth chain:
- `DATABRICKS_HOST` + `DATABRICKS_TOKEN`
- `DATABRICKS_CONFIG_PROFILE`
- Managed identity / OAuth on Databricks compute

## CLI Quick Reference

```bash
# Validate a config file
devmirror validate --config devmirror-config.yaml

# Scan streams and generate object manifest
devmirror scan --config devmirror-config.yaml --output manifest.yaml

# Provision DEV environment from manifest
devmirror provision --config devmirror-config.yaml --manifest manifest.yaml

# Provision with auto-approve (scan + provision in one step)
devmirror provision --config devmirror-config.yaml --auto-approve

# Refresh DEV data from production
devmirror refresh --dr-id DR-1042 --mode incremental

# Modify an active DR
devmirror modify --config devmirror-modify.yaml

# Manual cleanup of a DR
devmirror cleanup --dr-id DR-1042

# Show DR status
devmirror status --dr-id DR-1042

# List all active DRs
devmirror list
```

## Scheduled Jobs

DevMirror includes two scheduled entrypoints for automated lifecycle management:

| Job | Cron | Description |
|-----|------|-------------|
| **Pre-Expiry Notifications** | `0 8 * * *` (daily 8 AM UTC) | Sends notifications for DRs approaching expiration |
| **Expired DR Cleanup** | `0 2 * * *` (daily 2 AM UTC) | Drops objects, revokes grants, removes schemas for expired DRs |

### Running from Databricks Jobs

The scheduled entrypoints can be called directly from Python:

```python
from devmirror.jobs import run_notifications, run_cleanup

# Run in a Databricks job task
run_notifications()  # Pre-expiry notifications
run_cleanup()        # Expired DR cleanup
```

A Databricks Asset Bundle definition is provided in [`databricks.yml`](databricks.yml) for deployment:

```bash
databricks bundle validate
databricks bundle deploy --target dev
```

## Deploying to a new environment

Both the Databricks App (FastAPI + React UI) and the nightly lifecycle job
are managed by a single Databricks Asset Bundle (`databricks.yml`).  All
deploys go through `databricks bundle deploy`.

### One-time prerequisites per workspace

- A Databricks CLI profile authenticated to the target workspace
  (`databricks auth login --host <url> --profile <name>`).
- An **account group** the app uses for admin role -- defaults to
  `devmirror-admins`.  Members see the purple "Admin" badge.
- A running SQL warehouse (the deploy script auto-picks one, or pass
  `--warehouse-id`).
- For the prod DAB target, a dedicated service principal whose
  `application_id` is passed as `--run-as-sp <id>`.

### One-time bootstrap (first deploy to a workspace)

Run `scripts/bootstrap_env.sh` once.  It creates the Databricks App, waits
for compute, and grants the auto-created service principal `USE_CATALOG` +
`ALL_PRIVILEGES` on the control schema.  Pass `--smtp-password-secret` and
`--teams-webhook-secret` to also pre-create Secret app resources for the
Stage 5 notification backends.

```bash
./scripts/bootstrap_env.sh \
  --profile <profile> \
  --catalog dev_analytics \
  --schema devmirror_admin \
  --smtp-password-secret smtp-pw=devmirror/smtp-password \
  --teams-webhook-secret teams=devmirror/teams-webhook-url
```

### Adopting an app that already exists (one-time)

If the workspace already has a `devmirror` app (e.g. from a previous
deploy, or an app created manually via `databricks apps create`), bind it
to the bundle once so `databricks bundle deploy` updates it instead of
trying to create a duplicate:

```bash
databricks bundle deployment bind devmirror devmirror --auto-approve \
  --target dev --profile <profile> \
  --var warehouse_id=<id> --var node_type_id=i3.xlarge \
  --var control_catalog=dev_analytics --var control_schema=<sch>
```

The first `devmirror` is the bundle resource key; the second is the
existing app's name in the workspace.  After binding, normal
`./scripts/deploy.sh` runs work as expected.

### Routine deploys

```bash
# Dev (default target): auto-picks a warehouse, derives schema from your email.
./scripts/deploy.sh --profile <dev-profile>

# Prod: pin the SP that the lifecycle job runs as.
./scripts/deploy.sh \
  --profile <prod-profile> \
  --target prod \
  --warehouse-id <sql-warehouse-id> \
  --catalog dev_analytics \
  --schema devmirror_admin \
  --run-as-sp <prod-service-principal-application-id>
```

`scripts/deploy.sh` is a thin convenience wrapper.  It does:

1. `scripts/build_app.sh` -- builds the React frontend and stages the
   `devmirror` engine package into `app/devmirror/` so the bundle can
   upload one self-contained source tree.
2. `databricks bundle deploy` -- uploads source + registers the App
   resource and the lifecycle job.
3. `databricks bundle run devmirror` -- triggers an `apps deploy` to
   start serving the new source.

Equivalent manual flow:

```bash
./scripts/build_app.sh
databricks bundle deploy --target dev --profile <name> \
  --var warehouse_id=<id> --var node_type_id=i3.xlarge \
  --var control_catalog=dev_analytics --var control_schema=<sch> \
  --var admin_group=devmirror-admins
databricks bundle run devmirror --target dev --profile <name>
```

### SMTP / Teams notifications (Stage 5)

Notification delivery is optional.  After running `bootstrap_env.sh` with
`--smtp-password-secret` / `--teams-webhook-secret`, extend
`databricks.yml` under `resources.apps.devmirror.config.env` to reference
those resource keys via `valueFrom`, plus any plain values you need:

```yaml
resources:
  apps:
    devmirror:
      config:
        env:
          # ... existing 4 core vars ...
          - name: DEVMIRROR_SMTP_HOST
            value: smtp.example.com
          - name: DEVMIRROR_SMTP_FROM
            value: devmirror@example.com
          - name: DEVMIRROR_SMTP_USERNAME
            value: devmirror
          - name: DEVMIRROR_SMTP_PASSWORD
            valueFrom: smtp-pw
          - name: DEVMIRROR_TEAMS_WEBHOOK_URL
            valueFrom: teams
```

This is intentionally a YAML edit (not a CLI flag) so the deploy state is
captured in source control.  Promote between dev and prod via target
overrides.

### Bundle artifacts at a glance

| File | Purpose |
|---|---|
| `databricks.yml` | Single source of truth: lifecycle job + Databricks App |
| `scripts/bootstrap_env.sh` | One-time per-workspace setup (apps create, grants, secret resources) |
| `scripts/build_app.sh` | Pre-deploy: frontend build + devmirror staging |
| `scripts/deploy.sh` | Convenience wrapper: build + bundle deploy + bundle run |

## Security Model

1. **Principle of Least Privilege**: Developers only get access to objects within their DR scope.
2. **No PROD Write Access**: DevMirror never grants write access to production objects.
3. **Service Principal**: DevMirror runs as a service principal with elevated permissions to read PROD metadata/data and write to DEV. Individual developers do not need PROD access.
4. **Audit Trail**: All operations are logged in the audit table with user attribution.
5. **Expiration Enforcement**: DRs have a hard maximum lifetime. Extensions require explicit modification.

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run linting
python3 -m ruff check devmirror/ tests/

# Run tests
python3 -m pytest tests/unit/ -v --tb=short

# Auto-fix lint issues
python3 -m ruff check devmirror/ tests/ --fix
```

## Architecture

```
devmirror/
  cli.py                    # CLI entry point
  jobs.py                   # Scheduled job entrypoints
  config/                   # YAML config schema and loader
  scan/                     # Stream resolution, lineage, manifest
  provision/                # Schema creation, cloning, access grants
  refresh/                  # Data refresh engine
  modify/                   # DR modification engine
  cleanup/                  # Cleanup engine and pre-expiry notifier
  control/                  # Control table and audit repositories
  utils/                    # SQL executor, naming, validation, concurrency
```
