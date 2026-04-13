# Contracts: DevMirror

**Feature**: `001-devmirror-dr-cloning-lifecycle`

This directory holds machine-readable contracts for DevMirror v1. There is no first-party HTTP API in scope; operator contracts are **CLI commands** (see `cli-commands.md`) and **data contracts** (JSON Schema for YAML inputs and manifests).

| File | Purpose |
|------|---------|
| `config.schema.json` | JSON Schema describing `development_request` YAML shape (SPECIFICATION 2.1). |
| `manifest.schema.json` | JSON Schema describing `scan_result` / manifest shape (SPECIFICATION 3.1.2), including optional `approved_for_provision` after human review (WP06). |
| `cli-commands.md` | Stable CLI surface aligned with SPECIFICATION 5.3. |

DDL for Unity Catalog control tables should be added under this folder during implementation (for example `control-tables.sql`) so migrations stay versioned with the feature.
