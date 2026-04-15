# DevMirror App UI: User Stories -- Stage 3 (RBAC)

---

## US-23: See my role when using the app

**As a** user or admin,
**I want to** see my role displayed in the app header,
**So that** I know what actions are available to me.

**Acceptance Criteria:**
1. The app header shows the user's email and a role badge: "Admin" (purple) or "Developer" (blue).
2. The role is determined by Databricks group membership (configurable group name).
3. The role is fetched once on app load via `GET /api/me` and cached in React context.
4. If the role check fails, the user defaults to "Developer" (least privilege).

**Priority:** P0

---

## US-24: Create and edit my own configs as a developer

**As a** developer,
**I want to** create and edit development request configs,
**So that** I can submit requests for dev environments.

**Acceptance Criteria:**
1. I can create new configs via the form (same as today).
2. I can edit my own configs (where `created_by` matches my email).
3. I cannot edit configs created by other users.
4. Auto-scan runs after I save a valid config (same as today).
5. After saving, I see a banner: "Config saved. Pending admin review for provisioning."
6. I cannot see the "Re-provision" button on the form.

**Priority:** P0

---

## US-25: View scan results as read-only

**As a** developer,
**I want to** see which objects the scan discovered for my config,
**So that** I can verify the scope looks correct before an admin provisions it.

**Acceptance Criteria:**
1. After auto-scan completes, I can navigate to the scan results page for my config.
2. I see the objects table with FQN, type, access mode, estimated size, clone strategy.
3. I see the streams scanned and the total objects/schemas count.
4. I **cannot** remove objects from the manifest (no "Remove" button).
5. I **cannot** save changes to the manifest (no "Save Changes" button).
6. I **cannot** trigger provisioning (no "Approve & Provision" button).
7. A banner says: "Scan results are pending admin review."

**Priority:** P0

---

## US-26: See only my own configs and DRs

**As a** developer,
**I want to** see only configs and DRs that I created,
**So that** I'm not confused by other developers' requests.

**Acceptance Criteria:**
1. The config list shows only configs where `created_by` matches my email.
2. The active DRs list shows only DRs that I own.
3. I cannot access another user's config by URL (returns 403).
4. I cannot access another user's DR status by URL (returns 403).

**Priority:** P0

---

## US-27: Modify expiration and users on my provisioned DR

**As a** developer with an active DR,
**I want to** extend the expiration date or add/remove team members,
**So that** I can manage my dev environment without asking an admin.

**Acceptance Criteria:**
1. On my DR status page, I see a "Modify" button.
2. Clicking it opens a dialog with fields: Expiration Date, Add Developers, Remove Developers, Add QA Users, Remove QA Users.
3. Submitting calls `POST /api/drs/{id}/modify` with the changes.
4. The DR's expiration or access updates immediately.
5. Changes are logged in the audit trail.
6. I cannot add/remove objects or trigger re-scan (admin only).

**Priority:** P1

---

## US-28: Admin sees all configs and DRs

**As an** admin,
**I want to** see all configs and DRs across all users,
**So that** I can manage the full lifecycle for the team.

**Acceptance Criteria:**
1. The config list shows all configs regardless of who created them.
2. Each row shows the "Created By" column so I know who owns it.
3. The active DRs list shows all provisioned DRs.
4. I can access any config or DR by URL.

**Priority:** P0

---

## US-29: Admin triggers scan, reviews manifest, provisions

**As an** admin,
**I want to** trigger scans, review manifests, and provision DRs,
**So that** I can control when dev environments are created.

**Acceptance Criteria:**
1. On any config (regardless of owner), I see "Scan", "Review", "Provision" buttons.
2. On the scan results page, I can remove objects, save changes, and click "Approve & Provision".
3. I can re-scan a config that was already scanned.
4. I can provision, re-provision, refresh, and clean up any DR.
5. All my actions are logged in the audit trail with my email.

**Priority:** P0

---

## US-30: Admin triggers refresh and cleanup

**As an** admin,
**I want to** refresh dev data and clean up expired DRs,
**So that** dev environments stay current and unused ones are removed.

**Acceptance Criteria:**
1. On any active DR, I see "Refresh", "Re-provision", and "Cleanup" buttons.
2. Refresh supports incremental, full, and selective modes.
3. Cleanup shows a confirmation with object/schema/grant counts.
4. These buttons are not visible to regular users.

**Priority:** P0

---

## US-31: Unauthorized actions show clear error

**As a** developer trying to perform an admin action,
**I want to** see a clear "Permission denied" message,
**So that** I understand why I can't do something and who to ask.

**Acceptance Criteria:**
1. If I try to access an admin-only endpoint (e.g., provision), the backend returns 403 with message "Admin access required."
2. The frontend shows a toast: "This action requires admin access. Contact your platform team."
3. Admin-only buttons are not rendered at all for users (not just disabled).

**Priority:** P1

---

## US-32: Admin views another user's config with ownership label

**As an** admin editing a config I didn't create,
**I want to** see who owns the config,
**So that** I don't accidentally modify the wrong developer's request.

**Acceptance Criteria:**
1. When viewing/editing a config created by another user, a label shows: "Owner: user@company.com".
2. The label is subtle (not blocking) -- just informational.
3. The label appears on the config form, scan results, and DR status pages.

**Priority:** P2

---

## US-33: Configure admin group via app settings

**As a** platform administrator,
**I want to** set the admin group name via environment variable,
**So that** I can use my existing Databricks group structure.

**Acceptance Criteria:**
1. The `DEVMIRROR_ADMIN_GROUP` environment variable sets the group name (default: `devmirror-admins`).
2. If the group doesn't exist, all users are treated as regular developers (fail-safe).
3. Group membership is cached for 5 minutes to avoid repeated API calls.

**Priority:** P1

---

## Non-functional requirements

- **Fail-safe**: If role resolution fails (group API error), default to "user" (least privilege).
- **Performance**: Group membership cached 5 minutes. No per-request API call after initial check.
- **No UI for role management**: Roles managed through Databricks groups, not the app.
- **Audit attribution**: All audit entries include the actual user email, regardless of role.
