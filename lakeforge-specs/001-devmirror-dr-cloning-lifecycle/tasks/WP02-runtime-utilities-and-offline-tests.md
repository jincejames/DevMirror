---
work_package_id: "WP02"
title: "Runtime utilities and offline tests"
phase: "Phase 1 - Foundation"
lane: "planned"
dependencies:
  - "WP01"
subtasks:
  - "T006"
  - "T007"
  - "T008"
  - "T009"
  - "T010"
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

# Work Package Prompt: WP02 - Runtime utilities and offline tests

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- `sql_executor` can run a single SQL statement against a Databricks SQL warehouse when credentials are present (integration optional in CI).
- Naming helpers produce correct target schema names for dev and qa prefixes per isolation rules.
- Validation helpers enforce spec rules (expiration window, `dr_id` format, minimum developers).
- Settings load from environment variables with documented names.
- Offline unit tests pass without network.

## Context and constraints

- **Plan**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/plan.md` (utils layout, settings).
- **Research**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/research.md` (SDK usage, single admin schema).
- **Spec**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md` FR-001, edge cases for validation.
- **SPECIFICATION**: Sections 2.2, 3.2.1 naming, 4.1 isolation, 8 system config keys for defaults.

## Implementation command

```bash
lakeforge implement WP02 --base WP01
```

---

## Subtasks and detailed guidance

### Subtask T006 - sql_executor module

- **Purpose**: Centralize Databricks SQL execution for control DDL, lineage queries, and clone statements.
- **Steps**:
  1. Add `SqlExecutor` class accepting `WorkspaceClient` or building one from `databricks.sdk` using profile from `DATABRICKS_CONFIG_PROFILE` or default auth chain documented in class docstring.
  2. Method `execute_statement(sql: str, *, warehouse_id: str, wait_timeout: str | None = None) -> StatementResponse` using SQL Statement Execution API.
  3. Map common HTTP failures to typed exceptions `SqlExecutionError` carrying `statement_id` if available for support tickets.
  4. Provide optional helper to fetch result as list of dicts for small metadata queries.
- **Files**: `devmirror/utils/sql_executor.py`, update `devmirror/utils/__init__.py` exports if desired.
- **Parallel?**: After T009 settings exist for warehouse id resolution, or pass warehouse id explicitly into constructor to reduce coupling -> prefer explicit parameter plus optional settings resolver function in T009.
- **Validation**: Mock `WorkspaceClient` in unit tests (WP02 T010) to assert correct API path usage.

### Subtask T007 - naming module

- **Purpose**: Deterministic mapping from prod FQNs to dev or qa FQNs using `dr_id` numeric suffix rules in `SPECIFICATION.md` section 4.1-4.2.
- **Steps**:
  1. Parse numeric portion from `DR-1042` style ids; validate format.
  2. Functions: `dev_schema_prefix(dr_id) -> str` returning `dr_1042_` style (confirm underscore rules against SPEC examples `dr_1042_customers` -> prefix includes number without `DR-` prefix).
  3. `qa_schema_prefix(dr_id) -> str` returning `qa_1042_` per examples.
  4. `target_schema_fqn(catalog_dev: str, prod_schema_fqn: str, dr_id: str, env: Literal["dev","qa"]) -> str` building `{catalog}.{prefix}{original_schema}` per SPEC 3.2.1 examples.
  5. Expose helper to list required target schemas from a list of prod schema strings.
- **Files**: `devmirror/utils/naming.py`.
- **Parallel?**: Yes with T008 after shared types file if any.
- **Validation**: Golden tests for examples in SPECIFICATION 3.2.1 and 4.1.

### Subtask T008 - validation module

- **Purpose**: Shared validation beyond Pydantic for rules involving dates and policy limits.
- **Steps**:
  1. Function `validate_expiration(expiration: date, *, max_duration_days: int, today: date | None = None)` raising `ValidationError` subclass if past max.
  2. Function `validate_streams_resolved(...)` may belong in scan WP; here only keep cross-cutting config validation used before network calls.
  3. Ensure at least one developer email or group string (treat as opaque string per UC grant rules).
- **Files**: `devmirror/utils/validation.py`.
- **Parallel?**: Yes.
- **Validation**: Unit tests for boundary dates.

### Subtask T009 - settings module

- **Purpose**: Single place for environment-derived configuration used by executor and control repos.
- **Steps**:
  1. Define dataclass or frozen settings: `workspace_profile`, `warehouse_id`, `control_catalog`, `control_schema`, optional `http_path` if needed by chosen auth mode.
  2. Read from env vars with names documented in module docstring, for example `DEVMIRROR_CONTROL_CATALOG`, `DEVMIRROR_CONTROL_SCHEMA`, `DEVMIRROR_WAREHOUSE_ID` (exact names are implementer choice but must be documented in `README` in WP08).
  3. Provide `load_settings() -> Settings` raising clear error if required vars missing when a command needs them.
- **Files**: `devmirror/settings.py` (or `devmirror/config/settings.py` if you prefer colocation; stay consistent with plan tree `utils` vs `config` -> plan lists settings at package level as `devmirror/settings.py`).
- **Parallel?**: After T001.
- **Validation**: Missing required env raises `SettingsError`.

### Subtask T010 - offline unit tests

- **Purpose**: Lock behavior for naming, validation, and config parsing without Databricks.
- **Steps**:
  1. Create `tests/unit/test_naming.py` with table-driven cases from SPEC examples.
  2. Create `tests/unit/test_validation.py` for expiration boundaries.
  3. Create `tests/unit/test_config_loader.py` loading fixture YAML files under `tests/fixtures/config/` (add minimal valid and invalid samples).
  4. Add `tests/unit/test_sql_executor.py` mocking SDK client to assert statement API invocation shape.
- **Files**: `tests/unit/*.py`, `tests/fixtures/config/*.yaml`.
- **Parallel?**: After T006-T009 implementations exist.
- **Validation**: `pytest tests/unit` passes in CI without secrets.

## Test strategy

Run `pytest tests/unit -q` locally and in CI. No Databricks credentials required.

## Risks and mitigations

- **Risk**: Auth differences between local CLI and jobs. **Mitigation**: Document supported auth paths (profile, OAuth env vars) in module docstrings only until WP08 README.

## Review guidance

- Confirm tests do not call network.
- Confirm naming matches SPEC examples character for character for prefix pattern.

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
