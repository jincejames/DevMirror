# DevMirror App UI: Role-Based Access Control

## Specification v3.0 -- Stage 3

---

## 1. Overview

Stage 3 adds role-based access control (RBAC) to the DevMirror App, splitting functionality between two roles: **User** (developers requesting dev environments) and **Admin** (operations/platform team managing the lifecycle).

Users can create and edit configs, view scan results, and monitor their own DRs. Admins can do everything users can plus trigger scans, review manifests, provision, refresh, re-provision, and clean up.

### 1.1 Why RBAC

- Developers should submit requests without accidentally triggering expensive provisioning
- Scan review and provisioning are operational decisions that should be gated
- Cleanup of live environments should require admin privileges
- Audit trail needs to distinguish who performed what action

### 1.2 Roles

| Role | Who | Can Do |
|------|-----|--------|
| **User** | Any developer with app access | Create/edit/delete configs, view scan results (read-only), view DR status (read-only), export YAML, modify expiration/users on own DRs |
| **Admin** | Platform/operations team | Everything a User can do, plus: trigger scans, review & edit manifests, provision, re-provision, refresh, cleanup, view all DRs |

### 1.3 Role Assignment

Roles are determined by membership in Databricks groups:
- **Admin**: Members of a configurable group (default: `devmirror-admins`)
- **User**: Everyone else with app access

The admin group name is configured via the `DEVMIRROR_ADMIN_GROUP` environment variable in `app.yml`.

---

## 2. Backend Changes

### 2.1 Role Resolution

A new FastAPI dependency `get_user_role(request) -> str` that:
1. Extracts the user email from `X-Forwarded-Email` header
2. Queries the Databricks SDK `WorkspaceClient().groups.list(filter=f"displayName eq '{admin_group}'")` to check membership
3. Caches the result for 5 minutes (avoid repeated API calls per request)
4. Returns `"admin"` or `"user"`

### 2.2 Endpoint Access Matrix

| Endpoint | User | Admin |
|----------|------|-------|
| `POST /api/configs` (create) | Yes | Yes |
| `GET /api/configs` (list) | Own configs only | All configs |
| `GET /api/configs/{id}` (get) | Own only | All |
| `PUT /api/configs/{id}` (update) | Own only | All |
| `DELETE /api/configs/{id}` (delete) | Own only | All |
| `POST /api/configs/{id}/validate` | Own only | All |
| `GET /api/configs/{id}/yaml` (export) | Own only | All |
| `GET /api/streams/search` | Yes | Yes |
| `POST /api/configs/{id}/scan` (trigger scan) | No | Yes |
| `GET /api/configs/{id}/manifest` (view results) | Read-only (own) | Read/write |
| `PUT /api/configs/{id}/manifest` (edit manifest) | No | Yes |
| `POST /api/configs/{id}/provision` | No | Yes |
| `GET /api/tasks/{id}` (poll status) | Own tasks | All tasks |
| `GET /api/drs/{id}/status` | Own DRs | All DRs |
| `GET /api/drs` (list active DRs) | Own DRs | All DRs |
| `POST /api/drs/{id}/cleanup` | No | Yes |
| `POST /api/drs/{id}/refresh` | No | Yes |
| `POST /api/drs/{id}/reprovision` | No | Yes |

### 2.3 New Endpoint

#### `GET /api/me`

Returns the current user's identity and role.

**Response:**
```json
{
  "email": "dev@company.com",
  "role": "user",
  "display_name": "Dev User"
}
```

Used by the frontend to determine which UI elements to show.

### 2.4 Auto-Scan Behavior Change

Currently, auto-scan runs on every valid config create/update. With RBAC:
- Auto-scan still runs (so users see discovered objects)
- But the scan results page is **read-only** for users (no "Remove" button, no "Approve & Provision")
- Users can see what objects were discovered but cannot modify the manifest or trigger provisioning

### 2.5 Ownership

- `created_by` on the config row determines ownership
- Users can only see/edit configs where `created_by` matches their email
- Admins bypass ownership checks

### 2.6 User-Initiated Modifications on Provisioned DRs

Users can modify certain fields on their own provisioned DRs without admin involvement:
- Change expiration date (within policy limits)
- Add/remove developers and QA users (access management)

These changes call the existing `modify_dr()` engine function. A new endpoint is needed:

#### `POST /api/drs/{dr_id}/modify`

**Request body:**
```json
{
  "new_expiration_date": "2026-07-15",
  "add_developers": ["new.dev@company.com"],
  "remove_developers": [],
  "add_qa_users": [],
  "remove_qa_users": []
}
```

**Access:** Users (own DRs only) and Admins (all DRs).

---

## 3. Frontend Changes

### 3.1 Role-Aware Navigation

The app fetches `GET /api/me` on load and stores the role in React context. All pages check the role to show/hide actions.

### 3.2 User View

**Config List:**
- Shows only the user's own configs
- Actions: Edit, View Scan Results, Delete, YAML
- No "Scan", "Review", "Provision", "Re-provision" buttons
- Provisioned configs show: Edit, Status, YAML

**Config Form:**
- Full edit capability on own configs
- No "Re-provision" button (admin only)
- Shows a banner: "Submit to admin for provisioning" after saving

**Scan Results (read-only):**
- Shows the discovered objects table
- No "Remove" button on rows
- No "Save Changes" button
- No "Approve & Provision" button
- Banner: "Scan results are pending admin review"

**DR Status:**
- Shows status, objects, audit log (read-only)
- No "Cleanup", "Refresh", "Re-provision" buttons
- Shows a "Modify" button to change expiration/users

**DR List:**
- Shows only the user's own provisioned DRs

### 3.3 Admin View

Everything currently in the app, unchanged. Admins see all configs, all DRs, all action buttons.

### 3.4 Visual Indicators

- User's own configs/DRs: normal display
- Admin viewing another user's config: subtle "Owner: user@company.com" label
- Role badge in the header: "Admin" (purple) or "Developer" (blue)

---

## 4. App Configuration

### 4.1 Updated `app.yml`

```yaml
env:
  - name: DEVMIRROR_WAREHOUSE_ID
    value: <warehouse-id>
  - name: DEVMIRROR_CONTROL_CATALOG
    value: <catalog>
  - name: DEVMIRROR_CONTROL_SCHEMA
    value: <schema>
  - name: DEVMIRROR_ADMIN_GROUP
    value: devmirror-admins
```

### 4.2 Group Setup

Admins must create a Databricks group named `devmirror-admins` (or the configured name) and add platform team members to it. The app's service principal needs permission to read group memberships.

---

## 5. Implementation Approach

### 5.1 Backend

1. Add `get_user_role()` dependency with group membership check + cache
2. Add `GET /api/me` endpoint
3. Add `require_admin()` dependency that raises 403 if not admin
4. Add ownership check helper `require_owner_or_admin(row, user, role)`
5. Update list endpoints to filter by `created_by` for users
6. Wrap admin-only endpoints with `require_admin` dependency
7. Add `POST /api/drs/{dr_id}/modify` endpoint for user-initiated modifications

### 5.2 Frontend

1. Add `UserContext` with `{email, role}` fetched from `/api/me`
2. Add `useUser()` hook for components to check role
3. Conditionally render action buttons based on role
4. Add "Modify DR" dialog for users on the DR status page

---

## 6. Out of Scope (Stage 3)

- Fine-grained per-object permissions (all objects in a DR are accessible to the DR owner)
- Approval workflow (admin manually reviews -- no formal approve/reject tracking)
- Role assignment via the app UI (managed through Databricks groups)
- Multiple admin groups or tiered roles (only user/admin for now)
