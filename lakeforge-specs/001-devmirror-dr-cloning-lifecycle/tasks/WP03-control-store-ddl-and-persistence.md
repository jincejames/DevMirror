---
work_package_id: WP03
title: Control store DDL and persistence
lane: planned
dependencies: []
subtasks:
- T011
- T012
- T013
- T014
- T015
phase: Phase 1 - Foundation
assignee: ''
agent: ''
shell_pid: ''
review_status: ''
reviewed_by: ''
history:
- timestamp: '2026-04-13T20:00:00Z'
  lane: planned
  agent: system
  shell_pid: ''
  action: Prompt generated via /tasks
---

# Work Package Prompt: WP03 - Control store DDL and persistence

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- DDL exists for all control tables with column names and types aligned to `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md` and `SPECIFICATION.md` section 3.5.
- Python repositories can insert and update DR rows, bulk insert object rows, manage access rows, and append audit records using `SqlExecutor`.
- Operators have a documented, repeatable way to apply DDL in a fresh workspace.

## Context and constraints

- **Data model**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md`.
- **Research**: single-schema consolidation under `control_schema`.
- **SqlExecutor**: from WP02.
- **UC**: Use three-part names always. Prefer `STRING` columns for enums as in spec.

## Implementation command

```bash
lakeforge implement WP03 --base WP02
```

---

## Subtasks and detailed guidance

### Subtask T011 - Author control DDL

- **Purpose**: Versioned schema for metastore objects.
- **Steps**:
  1. Add SQL file with `CREATE TABLE IF NOT EXISTS` for `devmirror_development_requests`, `devmirror_dr_objects`, `devmirror_dr_access`, `audit_log` (table name per spec; if catalog also named `devmirror_admin` caused confusion, keep table name `audit_log` inside `control_schema` per research decision).
  2. Include primary key and foreign key constraints only if UC supports FKs in your target; if not, document application-level enforcement.
  3. Add indexes or liquid clustering only if justified; start minimal.
- **Files**: Prefer `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/control-tables.sql` AND copy or reference from `devmirror/migrations/001_control_tables.sql` if you want runtime-relative path -> pick one canonical location and reference the other via comment.
- **Parallel?**: Can start immediately after data-model review.
- **Validation**: Manual run in sandbox UC succeeds.

### Subtask T012 - DR repository methods

- **Purpose**: Persist lifecycle of each DR.
- **Steps**:
  1. Implement insert on create with status `PENDING_REVIEW` or `PROVISIONING` per chosen flow flag.
  2. Implement `update_status(dr_id, status, *, last_modified_at=now())`.
  3. Implement `get(dr_id)` returning typed dict or dataclass.
  4. Implement `list_active()` filtering statuses considered active for collision checks (WP06).
- **Files**: `devmirror/control/control_table.py` (start module; split files if size grows).
- **Parallel?**: After T011 column list is stable.
- **Validation**: Integration test optional; unit test with mocked `SqlExecutor` returning canned JSON rows.

### Subtask T013 - Object and access repository methods

- **Purpose**: Track each provisioned object and grants metadata.
- **Steps**:
  1. Bulk insert for manifest lines after successful clone or at row creation time depending on flow; store `source_fqn`, `target_fqn`, `clone_strategy`, revision fields.
  2. Update per-object `status` and `last_refreshed_at`.
  3. Replace access list for a DR environment by delete+insert or diff-based updates (document transactional limits).
- **Files**: `devmirror/control/control_table.py` or `devmirror/control/objects.py`.
- **Parallel?**: Sequential with T012 in same PR recommended.
- **Validation**: Mock executor verifies SQL contains expected placeholders.

### Subtask T014 - Audit repository

- **Purpose**: Immutable audit trail per FR-012.
- **Steps**:
  1. `append_audit(log_id, dr_id, action, detail_json, performed_by, status, error_message)` generating UUID for `log_id` if not supplied.
  2. `list_audit(dr_id, limit=500)` ordered descending for UI or CLI `status` later.
- **Files**: `devmirror/control/audit.py`.
- **Parallel?**: Yes after T011.
- **Validation**: Unit test append then list.

### Subtask T015 - DDL apply path and quickstart update

- **Purpose**: Make bootstrap reproducible for new workspaces.
- **Steps**:
  1. Either implement `scripts/apply_control_ddl.py` using `WorkspaceClient` and `sql_executor` reading SQL file contents, OR document step-by-step SQL editor workflow in `quickstart.md`.
  2. Update `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/quickstart.md` section "One-time platform setup" with exact variable names and order of operations.
- **Files**: `quickstart.md`, optional `scripts/apply_control_ddl.py`.
- **Parallel?**: After T011.
- **Validation**: New operator can follow quickstart without reading Python code.

## Test strategy

Prefer mocked SQL for unit tests; one optional integration test behind env flag `DEVMIRROR_RUN_INTEGRATION=1`.

## Risks and mitigations

- **Risk**: UC reserved keywords in table names. **Mitigation**: Quote identifiers in generated SQL if needed.

## Review guidance

- Verify DDL matches data-model field list exactly.
- Verify no destructive `DROP` in default bootstrap path.

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
