# Phase 0 Research: DevMirror DR cloning lifecycle

**Feature**: `001-devmirror-dr-cloning-lifecycle`  
**Date**: 2026-04-13  
**Sources**: `SPECIFICATION.md` v1.0, `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md`, engineering alignment: implement per specification as-is.

## 1. Runtime and packaging

- **Decision**: Ship a Python package `devmirror/` with `cli.py` (argparse or click), runnable from developer workstations and importable from Databricks notebooks; notebooks and scheduled Databricks Workflows call the same library for automation paths described in SPECIFICATION sections 6 and 3.6.
- **Rationale**: Single code path avoids drift between interactive and scheduled operations; matches section 5.1 (core logic in Python) and 5.2 module layout.
- **Alternatives considered**: Notebooks-only logic (rejected: harder to test, duplicate code); remote-only SaaS control plane (rejected: out of v1 scope).

## 2. Configuration validation

- **Decision**: Use Pydantic models in `config/schema.py` for in-memory validation after load; accept YAML via `loader.py` using PyYAML; optionally add strictyaml later for stricter document-level safety if required by security review.
- **Rationale**: SPECIFICATION names both PyYAML and strictyaml; Pydantic gives testable validation and clear errors for CLI users.
- **Alternatives considered**: strictyaml-only (rejected for v1: slower iteration without strong requirement); JSON configs only (rejected: spec is YAML-first).

## 3. Databricks integration surface

- **Decision**: Use `databricks-sdk` for Workflows API, Pipelines API, and SQL statement execution against a designated SQL warehouse; use Unity Catalog system tables (for example `system.access.table_lineage`) for lineage reads as in SPECIFICATION 3.1; Spark SQL only where SDK is insufficient and cluster context exists.
- **Rationale**: Aligns with technology stack table in SPECIFICATION 5.1.
- **Alternatives considered**: REST-only without SDK (rejected: more boilerplate, same security boundary).

## 4. Control store layout

- **Decision**: Implement all first-party control tables in one configurable schema (default name aligned with `devmirror_system_config.control_schema`, for example `devmirror_admin`) under `control_catalog`; store `devmirror_development_requests`, `devmirror_dr_objects`, `devmirror_dr_access`, and `audit_log` in that same schema for v1 to avoid cross-schema FK ambiguity. Document in plan that SPECIFICATION section 3.5.1 mentions `{dev_catalog}.utilities` while the audit table example uses `devmirror_admin` - implementation uses one admin schema from system config.
- **Rationale**: Resolves naming split in the source doc; keeps deployment configurable per section 8.
- **Alternatives considered**: Split `utilities` vs `devmirror_admin` (rejected until a hard requirement appears: increases migration and grant complexity).

## 5. Clone strategy defaults

- **Decision**: Encode default strategy selection per SPECIFICATION 3.2.2 table and section 8 `shallow_clone_threshold_gb`: READ_ONLY prefers view (zero dev storage); READ_WRITE uses shallow clone under threshold and deep or shallow per policy above threshold; WRITE_ONLY uses shallow or deep per size and mutability needs; allow manifest overrides after review.
- **Rationale**: Matches product intent on storage vs isolation tradeoffs.
- **Alternatives considered**: Deep clone all tables (rejected: cost); view for all reads including heavy transforms needing local mutation (rejected: views block mutation testing unless override).

## 6. Concurrency and orchestration

- **Decision**: Respect `max_parallel_clones` from system config using a worker pool in application code when issuing independent clone statements; serialize control table updates per `dr_id` to keep status consistent.
- **Rationale**: SPECIFICATION 8 and provisioning narrative in 6.1 step 6.
- **Alternatives considered**: Unlimited parallelism (rejected: risks warehouse throttling and partial failure storms).

## 7. Notifications

- **Decision**: Prefer Databricks notification destinations for scheduled notify tasks; allow optional SMTP integration behind a pluggable interface if destinations are unavailable in a workspace.
- **Rationale**: SPECIFICATION 3.6.2 and 5.1.
- **Alternatives considered**: Email-only via external service without Databricks integration (rejected as primary: extra secrets management).

## 8. Testing strategy (research outcome for Phase 1)

- **Decision**: Layer tests as: unit tests for config, naming, SQL string generation, and state machines with mocked SDK; integration tests gated on a non-prod workspace profile (manual or CI secret) for lineage and clone smoke paths; contract tests validating YAML and manifest JSON Schema examples under `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/`.
- **Rationale**: Platform feature requires SDK mocks for fast feedback plus at least one workspace for realistic UC behavior.
- **Alternatives considered**: Integration-only (rejected: slow and flaky for inner loop).

## Open items for `/tasks` (not blocking plan)

- Exact Databricks job bundle layout (DAB vs exported JSON) can be decided during implementation if not already standardized in repo.
