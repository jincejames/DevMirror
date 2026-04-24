# DevMirror App: Auto-Generated DR IDs, Change Audit, and Refresh Permissions

## Specification v4.0 -- Stage 4

---

## 1. Overview

Stage 4 introduces three related improvements to DevMirror:

1. **Auto-generated, zero-padded DR IDs** with a configurable prefix — end users no longer choose the DR number.
2. **Change auditing for config edits** — every change to key lifecycle / access fields is captured with before-and-after values and the user who made the change.
3. **Formal confirmation (and surfacing in docs) that developers can refresh their own shallow clones without operations approval** — this is already the code behaviour but is not yet explicit in the spec.

### 1.1 Why

- The `DR-1042` style user-chosen IDs have caused collisions and awkward sorting. Zero-padded auto-generated IDs (`DR00001`, `DR00002`, ...) sort lexically, guarantee uniqueness, and remove a step from the UX.
- Modifications to descriptions, expiration dates, developer lists, etc. are currently invisible — the `devmirror_configs` table only stores the latest state. Operations cannot see who changed what, when.
- Customers have asked whether developers need operations approval for refresh. The current code allows owners to refresh their own DRs; this should be stated plainly in the spec so it is not re-litigated.

---

## 2. DR ID Format

### 2.1 New Format

```
<PREFIX><zero-padded-counter>

Default: DR00001, DR00002, ..., DR12345, ...
```

- `PREFIX`: a short alphanumeric string. Default `DR`. Configurable per-deployment.
- `counter`: a monotonically increasing integer, zero-padded to a configurable width. Default 5 digits.
- No hyphen, no underscore, no user-chosen suffix.

### 2.2 Pattern Validation

The Pydantic regex in `devmirror/config/schema.py` is replaced:

```python
# Old:
DR_ID_PATTERN = re.compile(r"^DR-[0-9]+$")
# New (driven by env):
# For write paths: ^<PREFIX>[0-9]{PADDING}$
# For read paths:  accept both legacy DR-<digits> AND new <PREFIX><digits{PADDING}>
```

Legacy `DR-1042`-style IDs are accepted by read paths only (so the history remains queryable); new writes must use the new format.

### 2.3 Configuration (env vars)

| Env var | Default | Purpose |
|---|---|---|
| `DEVMIRROR_DR_ID_PREFIX` | `DR` | Alphanumeric prefix. Must match `^[A-Za-z][A-Za-z0-9]{0,7}$`. |
| `DEVMIRROR_DR_ID_PADDING` | `5` | Zero-padding width for the counter. Must be `>= 3` and `<= 12`. |

These are surfaced through `devmirror/settings.py::Settings` alongside the existing catalog / schema / admin group settings. Operators set them in `app.yaml` or CLI environment at **deployment time**, never from the end-user UI.

### 2.4 Counter Storage

A new table `devmirror_id_counter` in the control catalog:

```sql
CREATE TABLE IF NOT EXISTS <control_catalog>.<control_schema>.devmirror_id_counter (
  prefix           STRING NOT NULL,
  last_value       BIGINT NOT NULL,
  updated_at       TIMESTAMP NOT NULL
) USING DELTA;
```

One row per prefix. Atomic `UPDATE ... WHERE prefix = :p` with optimistic retry to generate the next ID.

### 2.5 Config Input Changes

- The `dr_id` field is **removed from `ConfigIn`** (the web form input model) and from the CLI `--dr-id` flag.
- The frontend `ConfigForm.tsx` no longer shows a DR ID input. The ID is assigned server-side at `POST /api/configs` time.
- `ConfigOut` still includes `dr_id` so the UI can display it after creation.
- The YAML config for the CLI still contains `dr_id` after the first save (so the file round-trips), but `dr_id` is populated by `devmirror` itself on the first validate/submit.

### 2.6 Backward Compatibility

- Existing `DR-1042`-style IDs remain valid for **reads**: list, get, scan-results, DR status, refresh, cleanup. No data migration.
- `POST /api/configs` rejects any caller-supplied `dr_id` with 400; the server assigns one.
- `UPDATE`/`DELETE` by legacy ID is still allowed so existing rows remain editable.

---

## 3. Config Change Audit

### 3.1 Scope

Changes to these fields are audited with before/after values:

- `description`
- `streams`
- `additional_objects`
- `lifecycle.expiration_date`
- `access.developers`
- `access.qa_users`

Structural fields (`dr_id`, `version`, `created_at`, `created_by`) are not audited because they do not change.

### 3.2 Where the Audit Is Written

The existing `devmirror_audit_log` table is reused. Its schema already has the needed columns (`action`, `action_detail` JSON, `performed_by`, `performed_at`, `status`, `error_message`). No migration needed.

Config edits write a new audit action `CONFIG_EDIT` with `action_detail` structured as:

```json
{
  "changes": [
    {
      "field": "lifecycle.expiration_date",
      "before": "2026-07-01",
      "after":  "2026-08-01"
    },
    {
      "field": "access.developers",
      "before": ["alice@co.com"],
      "after":  ["alice@co.com", "bob@co.com"]
    }
  ]
}
```

Only fields that actually changed appear in the `changes` array. If no audited field changed (e.g. the user only updated a non-audited field), no audit row is written.

### 3.3 Write Path

- `PUT /api/configs/{dr_id}` (`app/backend/router.py::update_config`) computes the diff between the stored config and the incoming config. For every changed audited field, it appends one `changes[]` entry, then writes a single audit row via `AuditRepository.append()`.
- `POST /api/drs/{dr_id}/modify` (`router_stage2.py::modify_dr_endpoint`) already writes audit entries through `modify_dr()` in the engine. Extend those entries to include before/after values for expiration and user-list changes, in the same `changes[]` shape.

### 3.4 Read Path

The existing `GET /api/drs/{dr_id}/status` already returns `recent_audit[]`. The frontend `DrStatus.tsx` already renders an audit log section but currently references the wrong field names (`timestamp`, `details`). As part of this stage:

- Fix field mapping: `performed_at` → displayed as timestamp, `action`, `action_detail` → parsed and rendered as a human-readable diff.
- Add a new element on `ConfigForm.tsx` showing "Recent Changes" when editing an existing config — same audit entries, filtered to the current DR.

### 3.5 Attribution

`performed_by` is set to the authenticated user's email (from `get_current_user`). This applies regardless of role (admin or user). Users cannot spoof this — the header is set by the Databricks Apps reverse proxy.

---

## 4. Refresh Without Approval

### 4.1 Confirmation of Current Behaviour

Today (as of Stage 3):

- `POST /api/drs/{dr_id}/refresh` does **not** require admin.
- It requires ownership (`require_owner_or_admin`) — a developer can refresh DRs they created; admins can refresh any DR.
- There is no approval queue, no second-party check, no blocking audit step.

### 4.2 What Stage 4 Adds

- An explicit documented acknowledgement in both the spec and the user-facing config YAML comments: **refresh is a self-service operation for the DR owner**.
- A new section in the App README (or an equivalent docs file) called "Refresh semantics" that clarifies:
  - Incremental refresh does not re-run the scan; it replays clones for previously-provisioned objects.
  - Full refresh drops + recreates cloned objects but does not change the object list.
  - Selective refresh only replays the user-chosen subset.
  - None of the three modes require admin approval when the caller owns the DR.
- Audit: every refresh writes a `REFRESH` audit entry (already the case via `refresh_dr()` engine). Stage 4 extends the `action_detail` to include the refresh mode and the count of objects touched, so operations can review after the fact.

### 4.3 Non-changes

No code paths are gated or added. This section is primarily a documentation change plus a small audit-detail enrichment.

---

## 5. Implementation Approach

### 5.1 Backend

1. Extend `Settings` in `devmirror/settings.py`:
   ```python
   dr_id_prefix: str = "DR"
   dr_id_padding: int = 5
   ```
   Read from env vars in `load_settings()` with existing pattern.

2. Create `devmirror/utils/id_generator.py`:
   - `next_dr_id(db_client, settings) -> str` — atomic increment via `UPDATE ... SET last_value = last_value + 1 WHERE prefix = :p` with read-back; bootstraps the row on first call.
   - `format_dr_id(prefix: str, counter: int, padding: int) -> str` — pure helper.
   - `is_legacy_dr_id(dr_id: str) -> bool` — returns `True` for `DR-<digits>` pattern.

3. Update `devmirror/config/schema.py`:
   - Make `DR_ID_PATTERN` configurable via a helper that reads the Settings prefix/padding.
   - Accept both legacy and new format on read; validate new format on write.
   - Change `DevelopmentRequest.dr_id` to accept either format; add a classmethod `validate_for_write` that rejects legacy.

4. Update `app/backend/router.py::create_config`:
   - Reject a `dr_id` on the request body with `400 DR ID is auto-generated; do not supply`.
   - Call `next_dr_id()` and inject into the row before insert.

5. Update `app/backend/router.py::update_config`:
   - Before writing the update, load the previous row, compute the audited-field diff, append one audit entry to `AuditRepository` if any changes.
   - Fields to diff: as listed in §3.1.

6. Update `devmirror/modify/modification_engine.py::modify_dr`:
   - Enrich `action_detail` for expiration and user-list actions to include before/after.

7. Add `devmirror/migrations/003_id_counter.sql` for the new table. Bootstrap path in the app lifespan and the CLI.

### 5.2 Frontend

1. Remove the `dr_id` input from `ConfigForm.tsx`. Show the generated ID as a read-only label after save.
2. Fix the audit-log rendering in `DrStatus.tsx`: use `performed_at`, `action`, parse `action_detail` JSON, render each `changes[]` entry as `<field>: before → after`.
3. Add a "Recent Changes" collapsible card to `ConfigForm.tsx` when editing an existing config.
4. Update `types.ts` `ConfigIn` to drop `dr_id`; keep it on `ConfigOut` and `ConfigListItem`.

### 5.3 CLI

1. Drop `--dr-id` from the `validate` / `scan` / `provision` commands that currently accept it. The YAML config still contains `dr_id` but it is injected on first save.
2. The CLI reads `DEVMIRROR_DR_ID_PREFIX` and `DEVMIRROR_DR_ID_PADDING` from env via `Settings`.

### 5.4 Tests

- Unit: `test_id_generator.py` — counter atomicity, padding correctness, prefix validation.
- Unit: extend `test_validation.py` for legacy regex compatibility.
- API: new tests in `test_router.py` — rejects user-supplied `dr_id`, generates incrementing IDs.
- API: new tests in `test_stage2.py` — config edits write audit entries with before/after diffs.
- UI: type check after form changes.

---

## 6. Out of Scope (Stage 4)

- Renaming or migrating existing `DR-1042` rows to the new format.
- Showing audit entries in a standalone search UI (the existing per-DR audit view is sufficient).
- Implementing a real email/notification backend for expirations (separate initiative).
- Approval workflow for refresh (explicitly not needed — see §4).
- Per-field retention policies for the audit table (the existing `audit_retention_days` setting applies uniformly).

---

## 7. Risks & Open Questions

| Risk | Mitigation |
|---|---|
| Concurrent `POST /api/configs` races on the counter | Use an optimistic-retry update on `devmirror_id_counter` with a short bounded retry loop (3 tries). Single-worker Uvicorn makes contention rare in practice. |
| Env-var misconfiguration (prefix with special chars) | Validate at `Settings` load time; fail fast with a clear message. |
| Padding too small (counter overflows width) | Width of 5 gives 100 000 DR IDs per prefix. Log a warning at 80 % capacity; operations can bump `DEVMIRROR_DR_ID_PADDING` and the pattern still accepts existing IDs. |
| Audit table row size | `action_detail` is a STRING field holding JSON. Developer/QA lists up to ~50 entries fit comfortably. Large `streams` or `additional_objects` edits may produce bigger rows; truncation is not implemented but can be added if this becomes a problem. |
