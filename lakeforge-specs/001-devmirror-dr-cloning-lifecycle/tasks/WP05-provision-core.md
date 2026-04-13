---
work_package_id: "WP05"
title: "Provision core"
phase: "Phase 3 - Provision"
lane: "planned"
dependencies:
  - "WP03"
  - "WP04"
subtasks:
  - "T022"
  - "T023"
  - "T024"
  - "T025"
  - "T026"
assignee: ""
agent: ""
shell_pid: ""
review_status: ""
reviewed_by: ""
history:
  - timestamp: "2026-04-13T20:00:00Z"
    lane: "planned"
    agent: "system"
    shell_pid: ""
    action: "Prompt generated via /tasks"
---

# Work Package Prompt: WP05 - Provision core

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- Prefixed dev (and optional qa) schemas exist before table creation.
- Clone SQL matches strategies in `SPECIFICATION.md` section 3.2.2 including revision clauses.
- Grants applied per isolation model without granting prod writes.
- Orchestration persists per-object outcomes to control tables and finishes DR status `ACTIVE` with warnings on partial failures when policy allows.
- CLI `devmirror provision` supports manifest path and `--auto-approve` path per contracts.

## Context and constraints

- **SPECIFICATION**: Sections 3.2, 6.1 steps 4-8, section 7 errors.
- **Data model**: `devmirror_dr_objects`, `devmirror_development_requests`.
- **Contracts**: `cli-commands.md`.
- **WP03**: repositories for inserts.
- **WP04**: manifest structure input.

## Implementation command

```bash
lakeforge implement WP05 --base WP04
```

If your workflow requires linear WP03 completion first, use:

```bash
lakeforge implement WP05 --base WP03
```

and ensure WP04 changes are already on the same branch. Prefer a single integration branch that merges WP03 then WP04 before WP05.

---

## Subtasks and detailed guidance

### Subtask T022 - schema_provisioner

- **Purpose**: Create isolated namespaces.
- **Steps**:
  1. Input: list of target schema FQNs from naming helper applied to `schemas_required` in manifest.
  2. Emit `CREATE SCHEMA IF NOT EXISTS` for each.
  3. Idempotent re-runs must not fail.
- **Files**: `devmirror/provision/schema_provisioner.py`.
- **Parallel?**: Safe across schemas but execute sequentially v1 to simplify error handling.
- **Validation**: SQL snapshot tests comparing generated strings.

### Subtask T023 - object_cloner

- **Purpose**: Generate clone DDL per object line.
- **Steps**:
  1. For each manifest object, choose strategy: start from manifest override if present else default mapping from `access_mode` and size threshold from system config (read from settings or static default until system table exists).
  2. Implement builders: `create_view_sql`, `create_shallow_clone_sql`, `create_deep_clone_sql`, `create_schema_only_sql` including `VERSION AS OF` / `TIMESTAMP AS OF` fragments from config `data_revision`.
  3. Never emit prod writes.
- **Files**: `devmirror/provision/object_cloner.py`.
- **Parallel?**: Pure functions parallelizable in review.
- **Validation**: Parameterized SQL tests for injection safety (identifiers must be quoted or validated allowlist pattern for three-part names).

### Subtask T024 - access_manager

- **Purpose**: Apply UC grants for developers and qa users on target schemas.
- **Steps**:
  1. Implement `grant_schema_rw(principal, schema_fqn)` using statements from `SPECIFICATION.md` 3.2.3.
  2. Implement `revoke_schema` used later by cleanup WP08 (stub revoke in WP05 if needed for symmetry).
  3. Accept principals as backtick-quoted identifiers per Databricks SQL docs.
- **Files**: `devmirror/provision/access_manager.py`.
- **Parallel?**: After T022 creates schemas.
- **Validation**: SQL string tests.

### Subtask T025 - provision runner orchestration

- **Purpose**: Coordinate multi-object execution with partial success.
- **Steps**:
  1. Create `devmirror/provision/runner.py` with `provision_dr(config, manifest, *, sql_executor, control, audit, max_parallel: int) -> ProvisionResult`.
  2. Sequence: insert DR `PROVISIONING`, insert planned object rows `REFRESH_PENDING` or similar, create schemas, execute clones with bounded parallelism (inline pool acceptable; WP07 refactors to shared helper).
  3. On per-object failure: capture exception text, mark object `FAILED`, continue unless fatal error (catalog missing) -> then abort DR as `FAILED`.
  4. On completion with any failures: DR `ACTIVE` with warning audit `PARTIAL_SUCCESS` if spec allows -> confirm against `SPECIFICATION.md` section 7 row "Object clone fails".
  5. Insert access rows and call `access_manager` after objects succeed or per-schema batching as you prefer.
- **Files**: `devmirror/provision/runner.py`.
- **Parallel?**: Single owner recommended.
- **Validation**: Dry-run mode optional: if implemented, must not mutate UC when flag set.

### Subtask T026 - CLI provision

- **Purpose**: Operator commands.
- **Steps**:
  1. Subcommands: `provision --config --manifest` and `provision --config --auto-approve` (auto path calls scan internally -> import scan pipeline functions).
  2. Wire settings, workspace client, sql executor, control repos, audit append for start and end.
  3. Print summary table of object outcomes to stdout for human operators.
- **Files**: `devmirror/cli.py`.
- **Parallel?**: After runner stable.
- **Validation**: Smoke with mocks end-to-end in unit test module `tests/unit/test_provision_cli_smoke.py` if feasible.

## Test strategy

SQL builder unit tests required. Integration provision test optional with workspace secrets.

## Risks and mitigations

- **Risk**: Identifier injection via malicious manifest. **Mitigation**: Validate FQN parts against `^[a-zA-Z0-9_]+$` or UC-safe pattern before interpolating.

## Review guidance

- Confirm no `MODIFY` or `DELETE` on prod paths in generated SQL.
- Confirm partial failure behavior matches spec section 7.

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
