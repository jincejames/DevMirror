---
work_package_id: "WP08"
title: "Lifecycle, remaining CLI, and ops docs"
phase: "Phase 5 - Lifecycle and operations"
lane: "planned"
dependencies:
  - "WP03"
  - "WP05"
  - "WP07"
subtasks:
  - "T038"
  - "T039"
  - "T040"
  - "T041"
  - "T042"
  - "T043"
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

# Work Package Prompt: WP08 - Lifecycle, remaining CLI, and ops docs

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- Expired DRs transition through cleanup states, drop dev artifacts in safe order, revoke grants, remove cloned jobs where tracked, and end in `CLEANED_UP` or retryable `CLEANUP_IN_PROGRESS` per `SPECIFICATION.md` section 3.6.
- Pre-expiry notifications fire once per DR when entering notification window unless policy extension is added later.
- CLI implements `cleanup`, `status`, `list` per contracts.
- Scheduled entrypoints exist for Databricks jobs with documented cron defaults from section 8.
- Root README explains setup, env vars, and links to feature docs.

## Context and constraints

- **SPECIFICATION**: Sections 3.6, 5.3 CLI, 8 config constants for cron.
- **WP03**: control and audit queries.
- **WP05**: access revoke symmetry, object drop order (views then tables then schemas CASCADE).
- **WP07**: modify may have added users; cleanup must revoke all grants issued for DR.

## Implementation command

```bash
lakeforge implement WP08 --base WP07
```

---

## Subtasks and detailed guidance

### Subtask T038 - cleanup_engine

- **Purpose**: Automated teardown for expired DRs.
- **Steps**:
  1. Query DRs with `expiration_date <= current_date` and `status = ACTIVE` (and handle `CLEANUP_IN_PROGRESS` retry path).
  2. Set status `CLEANUP_IN_PROGRESS` at start; record audit `CLEANUP` begin.
  3. Delete cloned jobs if ids stored; ignore missing id errors idempotently.
  4. Revoke grants issued for DR using stored access rows or inverse of grant log if you logged statements -> simplest: read `dr_access` and issue `REVOKE` mirrors of `GRANT` used in WP05-WP06.
  5. Drop views and tables for each `target_fqn` in dependency-safe order, then `DROP SCHEMA ... CASCADE` for prefixed schemas.
  6. Mark `CLEANED_UP` or leave `CLEANUP_IN_PROGRESS` with error on failure per spec partial cleanup behavior.
- **Files**: `devmirror/cleanup/cleanup_engine.py`.
- **Parallel?**: Sequential per DR; multiple DRs can run in job loop serially v1.
- **Validation**: Unit tests constructing SQL lists; integration optional.

### Subtask T039 - notifier

- **Purpose**: Pre-expiry emails or Databricks notifications.
- **Steps**:
  1. Select DRs where `expiration_date - notification_days <= today` and `notification_sent_at IS NULL` and `ACTIVE`.
  2. Compose message body including DR id, description, expiration, object count from control stats query.
  3. Send via Databricks Notifications or webhook stub -> implement `NotificationSender` protocol with at least one concrete class using documented SDK approach; SMTP optional second class behind feature flag env.
  4. Update `notification_sent_at` only after successful send.
- **Files**: `devmirror/cleanup/notifier.py`.
- **Parallel?**: After WP03 queries exist.
- **Validation**: Unit test with fake sender verifying idempotency.

### Subtask T040 - CLI cleanup, status, list

- **Purpose**: Complete CLI surface in contracts.
- **Steps**:
  1. `cleanup --dr-id` triggers cleanup engine for one DR (manual path).
  2. `status --dr-id` prints status, expiration, last refresh, object counts, last audit entries summary.
  3. `list` prints table of active DRs filtered appropriately.
- **Files**: `devmirror/cli.py`, maybe `devmirror/control/queries.py` for reporting SQL.
- **Parallel?**: After T038-T039 libraries exist.
- **Validation**: Mocked control data tests for formatting.

### Subtask T041 - scheduled entrypoints

- **Purpose**: Databricks jobs call Python without duplicating logic.
- **Steps**:
  1. Add module `devmirror/jobs.py` with functions `run_pre_expiry_notifications()` and `run_expired_cleanup()` calling notifier and cleanup engine using same settings loader as CLI.
  2. Guard each with try/except logging top-level failures for job observability.
- **Files**: `devmirror/jobs.py`.
- **Parallel?**: No.
- **Validation**: Invoke functions in unit test with mocks.

### Subtask T042 - Databricks bundle or job definitions

- **Purpose**: Ops-ready scheduling artifacts.
- **Steps**:
  1. Add `databricks.yml` or `databricks/` bundle with two jobs referencing `python -m devmirror.jobs notify` style commands OR notebook wrappers if org standard requires notebooks -> pick one and document.
  2. Wire cron defaults from SPEC section 8 (`cleanup_schedule_cron`, `notification_schedule_cron`) as job schedules.
  3. Do not commit secrets; use job parameters or default job cluster policy placeholders.
- **Files**: new under `databricks/` at repo root.
- **Parallel?**: With T043 if paths independent.
- **Validation**: `databricks bundle validate` if CLI available in CI optional.

### Subtask T043 - Root README

- **Purpose**: Onboarding for contributors and operators.
- **Steps**:
  1. Document install, env vars, link to `SPECIFICATION.md`, link to `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/quickstart.md`, link to feature spec and plan.
  2. Document security model: service principal, no prod writes, SELECT grants for views.
  3. Mention `lakeforge validate-encoding` for markdown contributors per `.lakeforge/AGENTS.md`.
- **Files**: `README.md` at repo root (create if missing).
- **Parallel?**: Yes.
- **Validation**: Human readability review.

## Test strategy

Notifier and cleanup heavily unit tested; one integration dry-run in staging workspace recommended before production enablement (outside this WP scope but document in README).

## Risks and mitigations

- **Risk**: Double notification if job retried incorrectly. **Mitigation**: DB field `notification_sent_at` guard and transactional update pattern documented in notifier code.

## Review guidance

- Verify drop order cannot leave orphan schemas with objects still referencing prod incorrectly (should be dev-only targets).
- Verify README has no secrets.

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
