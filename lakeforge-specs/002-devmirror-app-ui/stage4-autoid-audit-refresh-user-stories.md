# DevMirror App: User Stories -- Stage 4 (Auto-ID, Audit, Refresh)

---

## US-34: DR IDs are auto-generated so I never pick one

**As a** developer creating a new Development Request,
**I want to** not have to invent or remember a DR ID,
**So that** collisions are impossible and I can focus on describing the work.

**Acceptance Criteria:**
1. The config form no longer has a "DR ID" input field.
2. On save, the server assigns the next available ID (e.g. `DR00001`, `DR00002`).
3. The newly created DR displays its generated ID prominently on the config form and in the URL.
4. If I submit a YAML/API payload that includes `dr_id`, the server returns 400 with message "DR ID is auto-generated; do not supply".
5. IDs are zero-padded and sort lexically (so `DR00002` sorts before `DR00010`).

**Priority:** P0

---

## US-35: Operators configure the DR ID prefix and padding per deployment

**As a** platform administrator,
**I want to** set the DR ID prefix and padding width through environment variables,
**So that** different environments (dev / test / prod) or different teams can use visually distinct IDs.

**Acceptance Criteria:**
1. `DEVMIRROR_DR_ID_PREFIX` env var controls the prefix (default `DR`). Accepted values: alphanumeric, must start with a letter, max 8 characters.
2. `DEVMIRROR_DR_ID_PADDING` env var controls the zero-padding width (default `5`). Accepted range: 3 to 12.
3. Both values are read once at application startup (via `Settings`). Changing them requires a redeploy.
4. Invalid env var values cause the app to fail to start with a clear error message.
5. End users have no UI or API to change the prefix/padding.

**Priority:** P0

---

## US-36: Legacy `DR-xxxx` IDs remain readable

**As an** operator,
**I want to** continue viewing, refreshing, and cleaning up previously created DRs that use the old `DR-1042` format,
**So that** upgrading to the new ID scheme does not orphan existing environments.

**Acceptance Criteria:**
1. Any endpoint that reads a DR by ID (`GET /api/configs/{id}`, `GET /api/drs/{id}/status`, manifest, YAML export, refresh, reprovision, cleanup, modify) accepts both legacy `DR-<digits>` and new `<prefix><padded-digits>` formats.
2. The config list and DR list show both formats mixed, sorted by creation time.
3. The server does not rename legacy IDs. Existing dev catalogs and schemas keep their original names.
4. `POST /api/configs` (new DRs) always produces a new-format ID regardless of what the caller provides.

**Priority:** P1

---

## US-37: Changes to my DR are captured with before and after values

**As an** operator reviewing a customer request,
**I want to** see exactly what changed on a DR, who changed it, and when,
**So that** I can investigate issues and answer questions without running queries against the raw config table.

**Acceptance Criteria:**
1. When a config edit (`PUT /api/configs/{id}`) modifies any of:
   - `description`
   - `streams`
   - `additional_objects`
   - `lifecycle.expiration_date`
   - `access.developers`
   - `access.qa_users`
   an audit entry of action `CONFIG_EDIT` is written to the audit log with:
   - `performed_by`: the user's email
   - `performed_at`: UTC timestamp
   - `action_detail`: JSON array of `{field, before, after}` entries for each changed audited field.
2. Edits that only change non-audited fields (e.g. re-saving with identical content) do NOT write an audit row.
3. The `POST /api/drs/{id}/modify` endpoint already writes audit entries; those entries now also include before/after values for expiration and user-list changes, using the same `changes[]` shape.
4. All audit writes attribute the authenticated caller (never `SYSTEM`) for user-initiated modifications.

**Priority:** P0

---

## US-38: I can see the change history for a DR in the UI

**As a** developer or operator,
**I want to** see a timeline of edits on the config form and DR status page,
**So that** I can verify my own changes took effect and catch unexpected modifications.

**Acceptance Criteria:**
1. The config form (edit view) shows a collapsible "Recent Changes" section when editing an existing DR, rendering the most recent 20 audit entries of type `CONFIG_EDIT` or `MODIFY`.
2. Each audit entry renders as:
   - A timestamp (localized)
   - The performing user's email
   - A human-readable diff per field, e.g. `expiration_date: 2026-07-01 → 2026-08-01`.
3. The DR status page already has an audit log section; it is fixed to display the same formatted diff for `CONFIG_EDIT` and `MODIFY` entries (currently it references wrong field names and shows blank values).
4. If the audit log is empty, a "No recorded changes yet" placeholder is shown instead of an empty table.

**Priority:** P1

---

## US-39: Developers can refresh their own DRs without asking operations

**As a** developer with an active DR,
**I want to** refresh my cloned data from production at any time,
**So that** I can re-test against fresh data without waiting on an ops ticket.

**Acceptance Criteria:**
1. On my own DR status page, I see the "Refresh" button regardless of whether I am an admin (already true per US-30; Stage 4 confirms and documents this).
2. Submitting a refresh (incremental / full / selective) runs as a background task with no approval queue, no manual-review gate.
3. A refresh attempt on a DR I do not own returns 403 (unchanged).
4. Every refresh writes an audit entry of action `REFRESH` with `action_detail` including the mode and the count of objects touched.
5. The application docs (README or an equivalent operator-facing page) include a "Refresh semantics" section describing the three modes and noting that no admin approval is required for owners.

**Priority:** P1

---

## Non-functional requirements

- **Atomicity**: ID generation uses an optimistic-retry update on `devmirror_id_counter` (bounded to 3 retries). In single-worker mode this is effectively race-free; in multi-worker deployments it remains correct under contention.
- **Backward compatibility**: No existing DR row, control table, or dev catalog is modified by Stage 4.
- **Audit retention**: The existing `audit_retention_days` setting governs `CONFIG_EDIT` rows. No new retention policy is added.
- **Privacy**: `action_detail` stores user emails in before/after lists for `access.developers` / `access.qa_users`. This is acceptable because the audit log is already visible to admins and to owners of the DR.
