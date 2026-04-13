# Implementation Plan: DevMirror DR cloning lifecycle

**Branch**: `main` | **Date**: 2026-04-13 | **Spec**: [spec.md](./spec.md)  
**Input**: Feature specification from `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md`  
**Engineering alignment (confirmed)**: Implement per `SPECIFICATION.md` v1.0 as-is (stack, module layout, CLI, workflows).

## Summary

DevMirror automates discovery, review, provisioning, refresh, modification, notification, and cleanup of Unity Catalog-isolated replicas for development requests (DRs) tied to production Databricks streams. v1 delivers a Python package with CLI and notebook-callable modules, Databricks SQL and SDK integrations, UC-backed control tables, scheduled lifecycle jobs, and auditable state transitions, matching `SPECIFICATION.md` sections 2 through 8.

## Technical Context

**Language/Version**: Python 3.11+ (project environment reports 3.13; CI should cover 3.11 and 3.12 where feasible).

**Primary Dependencies**: `pyyaml` (required), `pydantic` for config models, `databricks-sdk` for Workflows, Pipelines, and SQL execution; optional `strictyaml` if stricter YAML loading is required later. CLI via `click` or `argparse` per implementation preference (SPECIFICATION allows either).

**Storage**: Unity Catalog control tables in configurable `control_catalog` + `control_schema` (see `research.md` for single-schema consolidation); developer data in dev/qa catalogs per DR prefixes.

**Testing**: `pytest` with unit tests for pure logic; mocked SDK for service calls; optional gated integration job against a non-prod workspace using workspace secrets.

**Target Platform**: Databricks (Unity Catalog, SQL warehouse, Workflows, Lakeflow Pipelines); operator CLI on macOS or Linux.

**Project Type**: Single Python package repository layout (new `devmirror/` tree at repo root when implementation starts).

**Performance Goals**: Clone throughput bounded by `max_parallel_clones` (default 10, SPECIFICATION 8); provisioning should tolerate large manifests without exceeding warehouse concurrency guidelines (tune during implementation).

**Constraints**: No writes to production data from DevMirror; single-metastore assumption (SPECIFICATION 10); partial success allowed on multi-object provision per SPECIFICATION 7; UTF-8 and ASCII-safe docs per `.lakeforge/AGENTS.md`.

**Scale/Scope**: Full v1 surface (scan, review, provision, refresh, modify, notify, cleanup, audit, list/status CLI) as in feature spec FR-001 through FR-012.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

**Skipped**: `.lakeforge/memory/constitution.md` is not present in this repository. No constitution gates applied.

**Post-design re-check**: No constitution conflicts identified.

## Project Structure

### Documentation (this feature)

```
lakeforge-specs/001-devmirror-dr-cloning-lifecycle/
├── plan.md
├── research.md
├── data-model.md
├── quickstart.md
├── contracts/
│   ├── README.md
│   ├── cli-commands.md
│   ├── config.schema.json
│   └── manifest.schema.json
├── spec.md
├── meta.json
├── checklists/
└── tasks/
```

`tasks.md` and work packages are produced by `/tasks`, not by `/plan`.

### Source Code (repository root) - target layout

Per `SPECIFICATION.md` section 5.2:

```
devmirror/
  __init__.py
  cli.py
  config/
    __init__.py
    schema.py
    loader.py
  scan/
    __init__.py
    lineage.py
    stream_resolver.py
    dependency_classifier.py
    manifest.py
  provision/
    __init__.py
    schema_provisioner.py
    object_cloner.py
    access_manager.py
    stream_cloner.py
  refresh/
    __init__.py
    refresh_engine.py
  modify/
    __init__.py
    modification_engine.py
  cleanup/
    __init__.py
    cleanup_engine.py
    notifier.py
  control/
    __init__.py
    control_table.py
    audit.py
  utils/
    __init__.py
    sql_executor.py
    naming.py
    validation.py

tests/
  unit/
  integration/
```

**Structure Decision**: Single package `devmirror/` with domain-driven subpackages above; tests mirror those boundaries. Packaging metadata (`pyproject.toml`, optional Databricks Asset Bundle for jobs) is added during implementation tasks.

## Complexity Tracking

*No constitution violations recorded; table not required.*

## Parallel Work Analysis

### Dependency Graph

```
Foundation: utils (sql_executor, naming, validation) + config (schema, loader)
    -> control (control_table, audit)
    -> scan (stream_resolver, lineage, dependency_classifier, manifest)
    -> provision (schema_provisioner, object_cloner, access_manager, stream_cloner)
    -> refresh (refresh_engine)
    -> modify (modification_engine)
    -> cleanup (cleanup_engine, notifier)
    -> cli (thin orchestration wiring all modules)
```

Scan and control can start after utils plus config validation. Provision depends on scan output shape and control persistence. Refresh and modify depend on provision artifacts. Cleanup and notifier depend on control and access metadata. CLI is last thin layer but stubs can exist early behind feature flags in development.

### Work Distribution

- **Sequential work**: Shared `utils.sql_executor`, `config.schema`, and `control` DDL alignment before real integration tests.
- **Parallel streams**: (1) scan lineage plus resolver, (2) provision clone SQL builders plus `access_manager`, (3) cleanup plus notifier once control APIs exist, (4) contracts and JSON Schema kept aligned with `config.schema.py` in tests.
- **Agent assignments**: Split by package folder (`scan/*`, `provision/*`, `cleanup/*`) to reduce merge conflicts.

### Coordination Points

- **Sync schedule**: Merge control table schema changes before parallel modules assume column names; use `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md` as reference.
- **Integration tests**: One golden-path DR in a sandbox workspace: scan -> provision -> refresh -> modify users -> cleanup dry-run.

## Phase 0: Research

**Status**: Complete. All technical unknowns for v1 are resolved against `SPECIFICATION.md` and recorded in `research.md` (no `[NEEDS CLARIFICATION]` markers in this plan).

**Artifact**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/research.md`

## Phase 1: Design and contracts

**Status**: Complete.

| Artifact | Path |
|----------|------|
| Data model | `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md` |
| Operator quickstart | `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/quickstart.md` |
| Contracts | `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/` |

REST or GraphQL public API is not in v1 scope; contracts are CLI documentation plus JSON Schema for YAML inputs and manifests.

## Agent context update

Run after this file is committed or at minimum present on disk:

`lakeforge agent context update-context --agent-type cursor --json`

from the repository root (or from this feature directory if required by the installed Lakeforge version). Parses `plan.md` Technical Context to refresh agent technology hints.

## Stop point

Per `/plan` command: **do not** create `tasks.md` or work packages here. Next user-driven command: `/tasks`.
