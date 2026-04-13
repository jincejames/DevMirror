---
work_package_id: "WP06"
title: "Stream clones and provisioning hardening"
phase: "Phase 3 - Provision"
lane: "planned"
dependencies:
  - "WP05"
subtasks:
  - "T027"
  - "T028"
  - "T029"
  - "T030"
  - "T031"
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

# Work Package Prompt: WP06 - Stream clones and provisioning hardening

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- Cloned or rewritten production jobs (first slice) exist for dev testing where stream cloning is in scope; pipeline support documented if deferred.
- View-on-prod strategy does not break due to missing SELECT on prod for dev principals when required by product rules.
- Auto-approve path cannot bypass human review when manifest flags `review_required`.
- Prefix collisions are detected using control store state before creating new schemas.
- Structured logs include `dr_id`, phase, and object context for support.

## Context and constraints

- **SPECIFICATION**: Sections 3.2.3 (views and revision), 3.6 cleanup mentions cloned workflows, 6.1 step 7 grants, 4 isolation, 7 errors for collisions.
- **Contracts**: extend `manifest.schema.json` in this WP for explicit approval boolean.
- **Depends on**: WP05 runner and CLI provision path.

## Implementation command

```bash
lakeforge implement WP06 --base WP05
```

---

## Subtasks and detailed guidance

### Subtask T027 - stream_cloner

- **Purpose**: Provide dev-executable copies of orchestration where required by product (SPEC cleanup drops cloned workflows).
- **Steps**:
  1. Implement `clone_job_for_dr(client, *, source_job_id, dr_id, name_suffix) -> new_job_id` duplicating job settings with rewritten parameters for target catalogs if job uses parameters; if not parameterizable, document limitation and require manual job template per org.
  2. For pipelines, either implement analogous clone using Pipelines API or raise `UnsupportedStreamType` with clear message and keep DR provision successful for data-only path -> product choice: prefer non-fatal stub with audit note `PARTIAL_SUCCESS` if pipeline clone not done.
  3. Persist cloned resource ids in control store if new columns needed -> add optional `cloned_job_ids` JSON column via follow-up migration in same WP or store in `config_yaml` temporarily -> document tradeoff.
- **Files**: `devmirror/provision/stream_cloner.py`.
- **Parallel?**: Mostly sequential integration work.
- **Validation**: Integration test optional; unit test with mocked SDK for job clone payload shape.

### Subtask T028 - Prod SELECT grants for view strategy

- **Purpose**: Satisfy access needs for views reading prod without copying data.
- **Steps**:
  1. When `clone_strategy` is `view`, ensure `access_manager` can issue `GRANT SELECT ON TABLE prod.fqn TO principal` for each developer and qa principal per SPEC provisioning narrative.
  2. Never grant `MODIFY` on prod tables.
  3. Log each grant in audit detail JSON.
- **Files**: `devmirror/provision/access_manager.py`, possibly `runner.py` call order.
- **Parallel?**: After T027 if grants depend on knowing principals list only -> can parallelize conceptually with T027 if separate files.
- **Validation**: SQL review checklist in PR description.

### Subtask T029 - Auto-approve safety

- **Purpose**: Prevent silent bypass of review when lineage incomplete.
- **Steps**:
  1. Extend manifest schema with `approved_for_provision: boolean` default false.
  2. In `--auto-approve` path, allow auto true only when `review_required` is false.
  3. When `review_required` true, require manifest file on disk with `approved_for_provision: true` set by reviewer tool or manual edit; CLI must read merged manifest after human edits.
  4. Update `contracts/manifest.schema.json` and `contracts/README.md`.
- **Files**: `contracts/manifest.schema.json`, `devmirror/cli.py`, `devmirror/provision/runner.py` validation gate.
- **Parallel?**: No.
- **Validation**: Unit test: `review_required` true and missing approval raises error before any DDL.

### Subtask T030 - Prefix collision detection

- **Purpose**: Surface conflict with existing active DR per spec section 7.
- **Steps**:
  1. Before schema creation, query control tables for other DRs with overlapping target schema FQNs and statuses in active set (`ACTIVE`, `PROVISIONING`, etc. define explicitly in code constant).
  2. Raise `PrefixCollisionError` including incumbent `dr_id`.
- **Files**: `devmirror/provision/runner.py` or `devmirror/utils/naming.py` helper used by runner.
- **Parallel?**: No.
- **Validation**: Unit test with mocked control query.

### Subtask T031 - Structured logging

- **Purpose**: Operations triage for large manifests.
- **Steps**:
  1. Use stdlib `logging` with logger name `devmirror`.
  2. Include extra fields via `LoggerAdapter` or structured `dict` messages if you use `python-json-logger` (optional dependency) -> default to key=value strings in message to avoid new deps.
  3. Ensure no secrets logged (tokens, passwords).
- **Files**: across `provision/runner.py`, `cli.py`, `scan/*` as needed.
- **Parallel?**: Incremental commits OK.
- **Validation**: Manual log review sample in PR.

## Test strategy

Focus on unit tests for gates (T029, T030). Integration for T027 optional.

## Risks and mitigations

- **Risk**: Jobs API payload too large to clone. **Mitigation**: Catch API error, record `PARTIAL_SUCCESS`, document size limits.

## Review guidance

- Confirm prod grants only SELECT and only when view strategy is used.
- Confirm manifest schema change is backward compatible with examples (default false).

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
