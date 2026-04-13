---
work_package_id: WP07
title: Refresh, modify, and concurrency
lane: planned
dependencies: []
subtasks:
- T032
- T033
- T034
- T035
- T036
- T037
phase: Phase 4 - Change management
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

# Work Package Prompt: WP07 - Refresh, modify, and concurrency

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- Refresh updates cloned objects and view definitions to requested revision or latest per `SPECIFICATION.md` section 6.2.
- Modify applies deltas for objects, schemas, users, dates, and added streams with partial success semantics per section 3.4.
- CLI exposes `refresh` and `modify` per contracts.
- Audit entries record batch outcomes including partial success.
- Bounded concurrency helper is shared by provision runner and refresh engine after refactor.

## Context and constraints

- **SPECIFICATION**: Sections 3.3, 3.4, 6.2, 8 `max_parallel_clones`.
- **Contracts**: `cli-commands.md`.
- **Depends on**: WP05 provision runner and object SQL builders (reuse `object_cloner` for `CREATE OR REPLACE` patterns).

## Implementation command

```bash
lakeforge implement WP07 --base WP05
```

---

## Subtasks and detailed guidance

### Subtask T032 - refresh_engine

- **Purpose**: Re-sync data without full reprovision.
- **Steps**:
  1. Implement modes `full`, `incremental`, `selective` per SPEC 6.2: define exact meaning in code (for example incremental refreshes only non-view clones).
  2. Validate DR status is `ACTIVE` and not expired before work.
  3. Validate revision within retention: on failure, query Delta `DESCRIBE HISTORY` is expensive -> optional best-effort or fail fast with message to operator to verify manually in v1; document limitation if not implemented.
  4. For each target object row in control store, run appropriate SQL via `SqlExecutor` using same builders as provision with `OR REPLACE` variants.
  5. Update `last_refreshed_at` on DR and each object row.
- **Files**: `devmirror/refresh/refresh_engine.py`.
- **Parallel?**: Internal object-level parallelism uses helper from T037 once extracted; until then reuse runner pool pattern.
- **Validation**: Unit tests with mocked executor capturing SQL order.

### Subtask T033 - CLI refresh

- **Purpose**: Operator entrypoint for refresh flows.
- **Steps**:
  1. Parse args per `SPECIFICATION.md` 5.3 examples (`--dr-id`, `--mode`, `--revision` or config file path).
  2. Load DR from control, deserialize manifest or rely on object rows only -> prefer object rows as source of truth post-provision.
  3. Call refresh engine and print summary.
- **Files**: `devmirror/cli.py`.
- **Parallel?**: After T032.
- **Validation**: Smoke tests with mocks.

### Subtask T034 - modification_engine

- **Purpose**: Support lifecycle edits without full teardown per table in SPEC 3.4.1.
- **Steps**:
  1. Load current DR state from control.
  2. Parse modify YAML same schema family as create per SPEC 3.4.2 (reuse Pydantic models with optional sections or separate models with shared pieces).
  3. Implement actions: add objects (scan+provision delta), remove objects (drop dev targets + delete rows), add/remove users (grant/revoke), change expiration (update row + reschedule semantics documented only), add streams (merge manifests then provision new only).
  4. Continue on per-action failure where spec says partial success model -> record per-action errors in audit `PARTIAL_SUCCESS`.
- **Files**: `devmirror/modify/modification_engine.py`.
- **Parallel?**: Single owner.
- **Validation**: Unit tests per action with mocked control and sql.

### Subtask T035 - CLI modify

- **Purpose**: Operator entrypoint.
- **Steps**:
  1. `modify --config path` loads modify yaml.
  2. Dispatch to modification engine.
- **Files**: `devmirror/cli.py`.
- **Parallel?**: After T034.
- **Validation**: Smoke tests.

### Subtask T036 - Audit integration for refresh and modify

- **Purpose**: Meet FR-012 for these operations.
- **Steps**:
  1. Ensure each refresh run writes audit with JSON summary counts success vs fail.
  2. Each modify batch writes audit with action list and status.
- **Files**: `devmirror/refresh/refresh_engine.py`, `devmirror/modify/modification_engine.py`.
- **Parallel?**: Small edits alongside T032-T035.
- **Validation**: Assert audit append calls in unit tests.

### Subtask T037 - concurrent helper refactor

- **Purpose**: Centralize parallelism policy.
- **Steps**:
  1. Add `devmirror/utils/concurrent.py` with `run_bounded(max_workers: int, tasks: Iterable[Callable[[], None]])` or async-free `ThreadPoolExecutor` wrapper collecting exceptions.
  2. Refactor `provision/runner.py` to use helper.
  3. Refactor `refresh_engine` to use helper.
  4. Read default max workers from settings env `DEVMIRROR_MAX_PARALLEL_CLONES` fallback 10.
- **Files**: `devmirror/utils/concurrent.py`, `devmirror/provision/runner.py`, `devmirror/refresh/refresh_engine.py`, `devmirror/settings.py`.
- **Parallel?**: Refactor after T032 exists.
- **Validation**: Unit test that max concurrency is not exceeded using timing or counter instrument (lightweight).

## Test strategy

Unit tests for engines and CLI with mocks; no integration requirement.

## Risks and mitigations

- **Risk**: `CREATE OR REPLACE` locks. **Mitigation**: Document off-hours refresh; optional per-object retry count.

## Review guidance

- Confirm modify partial failures do not corrupt control store (use transactions per action where possible).

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
