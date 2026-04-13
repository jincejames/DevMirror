# Quickstart: DevMirror (implementation target)

**Feature**: `001-devmirror-dr-cloning-lifecycle`  
**Date**: 2026-04-13

This quickstart describes how operators and developers will use DevMirror once the package from `SPECIFICATION.md` section 5 exists. Paths are relative to the repository root unless absolute.

## Prerequisites

1. Databricks workspace with Unity Catalog, SQL warehouse, and permissions for a DevMirror service principal to read production metadata (and data for clone operations), plus create objects in dev catalogs.
2. Databricks CLI or environment variables for workspace authentication (profile used by `databricks-sdk`).
3. Python 3.11+ with project dependencies installed (see future `pyproject.toml` at repo root when added during implementation).

## One-time platform setup

1. Create UC catalog and storage for dev workloads if not already present (per your org standards).
2. Create control schema (default name from `devmirror_system_config.control_schema`, for example `devmirror_admin`) in `control_catalog`.
3. Apply DDL for `devmirror_development_requests`, `devmirror_dr_objects`, `devmirror_dr_access`, and `audit_log` (contracts under `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/` will hold reference definitions during implementation).
4. Register two scheduled workflows using the same package entrypoints: notification pass (SPECIFICATION 3.6.2) and cleanup pass (3.6.1, 3.6.3), with cron from system config.

## Operator flow (happy path)

1. Author `devmirror-config.yaml` following `contracts/config.schema.json`.
2. Run scan: `devmirror scan --config devmirror-config.yaml --output manifest.yaml` (SPECIFICATION 5.3).
3. Review and edit `manifest.yaml`; approve.
4. Run provision: `devmirror provision --config devmirror-config.yaml --manifest manifest.yaml`.
5. Confirm DR row is `ACTIVE` and developers can query `target_fqn` objects.

## Developer refresh

- `devmirror refresh --dr-id DR-1042 --mode incremental --revision latest` or use a refresh YAML as in SPECIFICATION 5.3.

## Inspect status

- `devmirror status --dr-id DR-1042` and `devmirror list`.

## Where to read more

- Product behavior: `SPECIFICATION.md`
- Formal requirements: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md`
- Engineering plan: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/plan.md`
