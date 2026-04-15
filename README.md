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
