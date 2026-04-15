# DevMirror App UI: Scan, Provision & Lifecycle

## Specification v2.0 -- Stage 2

---

## 1. Overview

Stage 2 extends the DevMirror App beyond configuration management (Stage 1) to cover the full operational lifecycle: scanning production streams to discover objects, reviewing the scan results, provisioning isolated dev copies, and automated background cleanup of expired requests.

The app becomes the single interface for the entire DR workflow -- no CLI needed.

### 1.1 What Stage 1 Already Provides

- Create / edit / delete DR configurations via web form
- Validate configs against schema and policy rules
- Export configs as YAML
- Stream name typeahead search
- Config storage in `devmirror_configs` Delta table

### 1.2 What Stage 2 Adds

- **Scan**: Trigger object discovery from the app, view discovered objects
- **Review**: Approve or modify the scan manifest before provisioning
- **Provision**: Clone objects to dev/qa from the app with live progress
- **Status & Monitoring**: Track DR lifecycle state, view objects and audit history
- **Background Cleanup**: Expired DRs cleaned up automatically via scheduled task

### 1.3 Execution Model

Databricks Apps have no SparkSession. All SQL goes through the Statement Execution API via `DbClient`. Long-running operations (scan + provision can take minutes) are executed asynchronously:

1. User triggers an action (scan / provision / cleanup)
2. Backend starts a background thread, returns immediately with a job ID
3. Frontend polls for status until complete
4. Results are stored in control tables and displayed in the UI

---

## 2. Backend API -- New Endpoints

All endpoints reuse existing DevMirror engine functions directly. No logic duplication.

### 2.1 Scan Endpoints

#### `POST /api/configs/{dr_id}/scan`

Trigger an object discovery scan for a saved config.

**What it does:**
1. Loads the config from `devmirror_configs`
2. Resolves streams via `resolve_streams()`
3. Queries lineage via `query_lineage()`
4. Classifies dependencies via `classify_dependencies()`
5. Builds manifest via `build_manifest()`
6. Stores the manifest JSON in a new `scan_status` column on the config row
7. Updates config status to `scanned`

**Execution:** Synchronous for now (scan typically completes in 5-30 seconds). If too slow, move to background thread in a later iteration.

**Response (200):**
```json
{
  "dr_id": "DR-1042",
  "status": "scanned",
  "manifest": {
    "scan_result": {
      "dr_id": "DR-1042",
      "scanned_at": "2026-04-15T10:00:00Z",
      "streams_scanned": [...],
      "objects": [
        {"fqn": "prod.schema.table", "type": "table", "access_mode": "READ_ONLY", "estimated_size_gb": 1.2},
        ...
      ],
      "schemas_required": ["prod.schema"],
      "total_objects": 5,
      "review_required": false
    }
  }
}
```

**Error (400):** If streams can't be resolved.

#### `GET /api/configs/{dr_id}/manifest`

Retrieve the stored scan manifest for review.

**Response (200):** The manifest JSON stored during the scan.
**Response (404):** If no scan has been run yet.

#### `PUT /api/configs/{dr_id}/manifest`

Update the manifest after human review (add/remove objects, override clone strategies).

**Request body:** The modified manifest dict.
**Response (200):** Updated manifest.

### 2.2 Provision Endpoints

#### `POST /api/configs/{dr_id}/provision`

Trigger provisioning of dev/qa objects from an approved manifest.

**What it does:**
1. Loads config and manifest from storage
2. Calls `provision_dr()` with the config and manifest
3. Records result in control tables (`devmirror_development_requests`, `devmirror_dr_objects`, etc.)
4. Updates config status to `provisioned`

**Execution:** Can take 1-5 minutes for large manifests. Runs in a background thread. Returns immediately with a task ID.

**Response (202):**
```json
{
  "dr_id": "DR-1042",
  "task_id": "task-abc123",
  "status": "provisioning",
  "message": "Provisioning started. Poll GET /api/tasks/{task_id} for progress."
}
```

#### `GET /api/tasks/{task_id}`

Poll for background task status.

**Response (200):**
```json
{
  "task_id": "task-abc123",
  "dr_id": "DR-1042",
  "type": "provision",
  "status": "running",
  "progress": "Cloning objects (3/5)...",
  "started_at": "2026-04-15T10:01:00Z"
}
```

When complete:
```json
{
  "task_id": "task-abc123",
  "dr_id": "DR-1042",
  "type": "provision",
  "status": "completed",
  "result": {
    "final_status": "ACTIVE",
    "objects_succeeded": 5,
    "objects_failed": 0,
    "schemas_created": 2,
    "grants_applied": 4
  },
  "started_at": "2026-04-15T10:01:00Z",
  "completed_at": "2026-04-15T10:03:30Z"
}
```

### 2.3 Status & Monitoring Endpoints

#### `GET /api/drs/{dr_id}/status`

Get the full lifecycle status of a provisioned DR.

**Response (200):**
```json
{
  "dr_id": "DR-1042",
  "status": "ACTIVE",
  "description": "Customer churn pipeline",
  "expiration_date": "2026-06-15",
  "created_at": "2026-04-15T10:00:00Z",
  "last_refreshed_at": null,
  "objects": [
    {"source_fqn": "prod.schema.table", "target_fqn": "dev.dr_1042_schema.table", "status": "PROVISIONED", "clone_strategy": "shallow_clone"}
  ],
  "total_objects": 5,
  "object_breakdown": {"PROVISIONED": 5},
  "recent_audit": [
    {"action": "PROVISION", "status": "SUCCESS", "performed_at": "2026-04-15T10:03:30Z"}
  ]
}
```

#### `GET /api/drs`

List all provisioned DRs (from the control table, not configs table).

### 2.4 Cleanup Endpoint

#### `POST /api/drs/{dr_id}/cleanup`

Manually trigger cleanup for a specific DR.

**What it does:** Calls `cleanup_dr()` which drops objects, revokes grants, removes schemas.

**Response (200):**
```json
{
  "dr_id": "DR-1042",
  "final_status": "CLEANED_UP",
  "objects_dropped": 5,
  "schemas_dropped": 2,
  "revokes_succeeded": 4
}
```

---

## 3. Background Task System

Since provision and cleanup can take minutes, the app needs a simple in-memory task tracker.

### 3.1 Task Tracker

```python
class TaskTracker:
    """In-memory background task tracker stored on app.state."""

    def submit(self, dr_id: str, task_type: str, fn: Callable) -> str:
        """Start fn in a background thread, return task_id."""

    def get(self, task_id: str) -> TaskStatus | None:
        """Get current task status."""

    def list_for_dr(self, dr_id: str) -> list[TaskStatus]:
        """Get all tasks for a DR."""
```

Tasks are ephemeral (in-memory only). If the app restarts, running tasks are lost. The actual DR state is always in the control tables, so no data is lost -- the user just needs to re-trigger if a provision was interrupted.

### 3.2 Config Status Lifecycle

The config row's `status` column tracks the workflow stage:

```
valid -> scanned -> reviewed -> provisioned
  |                    |
  v                    v
invalid            (back to scanned if re-scanned)
```

---

## 4. Frontend -- New Pages & Components

### 4.1 Updated Config List Page

Add columns: **Scan Status** (not scanned / scanned / provisioned), **Actions** (Scan / Review / Provision / Status).

### 4.2 Scan Results Page (`/config/{dr_id}/scan`)

Shows the scan manifest in a readable table:

| Object FQN | Type | Access Mode | Size (GB) | Strategy |
|------------|------|-------------|-----------|----------|
| prod.schema.table1 | table | READ_ONLY | 1.2 | shallow_clone |
| prod.schema.table2 | table | READ_WRITE | 0.5 | shallow_clone |

**Actions:**
- "Approve & Provision" -- triggers provisioning
- "Remove" per object -- removes from manifest
- "Back to Config" -- returns to the form

Shows `review_required` banner if flagged by the scan engine.

### 4.3 Provision Progress Page (`/config/{dr_id}/provision`)

- Progress bar or status text ("Cloning objects 3/5...")
- Polls `GET /api/tasks/{task_id}` every 3 seconds
- On completion, shows summary (objects created, grants applied, failures)
- "View DR Status" button on completion

### 4.4 DR Status Page (`/dr/{dr_id}`)

Full lifecycle view of a provisioned DR:
- Status badge (ACTIVE / EXPIRING_SOON / CLEANUP_IN_PROGRESS / CLEANED_UP)
- Object table with source -> target mapping and per-object status
- Audit log timeline
- Actions: "Cleanup" button (with confirmation dialog)

### 4.5 DR List Page (`/drs`)

Table of all provisioned DRs from the control table. Columns: DR ID, Status, Expiration, Object Count, Last Refreshed.

---

## 5. Engine Functions Reused

Every operation maps directly to an existing DevMirror function:

| App Action | Engine Function | Module |
|-----------|----------------|--------|
| Scan | `resolve_streams()` + `query_lineage()` + `classify_dependencies()` + `build_manifest()` | `scan/` |
| Provision | `provision_dr(config, manifest, ...)` | `provision/runner.py` |
| Cleanup | `cleanup_dr(dr_id, ...)` | `cleanup/cleanup_engine.py` |
| DR Status | `DRRepository.get()` + `DrObjectRepository.list_by_dr_id()` + `AuditRepository.list_by_dr_id()` | `control/` |
| DR List | `DRRepository.list_active()` | `control/control_table.py` |
| Find Expired | `find_expired_drs()` | `cleanup/cleanup_engine.py` |

Zero new engine logic. The app is purely a UI + API layer.

---

## 6. Storage Changes

### 6.1 Add columns to `devmirror_configs`

| Column | Type | Purpose |
|--------|------|---------|
| `manifest_json` | STRING | Scan manifest stored as JSON |
| `scanned_at` | STRING | When the last scan was run |

Migration: `ALTER TABLE ... ADD COLUMNS` at app startup.

### 6.2 Existing Control Tables (no changes)

- `devmirror_development_requests` -- DR lifecycle state
- `devmirror_dr_objects` -- per-object clone status
- `devmirror_dr_access` -- user grants
- `audit_log` -- operation history

---

## 7. Background Cleanup

A lightweight background loop runs inside the app process:

```python
async def cleanup_loop():
    """Run every 6 hours: find expired DRs, clean up each."""
    while True:
        await asyncio.sleep(6 * 3600)
        try:
            expired = find_expired_drs(db_client, dr_repo)
            for dr_row in expired:
                cleanup_dr(dr_row["dr_id"], ...)
        except Exception:
            logger.error("Background cleanup failed", exc_info=True)
```

Started in the FastAPI lifespan. Supplements (does not replace) the Databricks Job-based cleanup.

---

## 8. Out of Scope (Stage 2)

- Refresh from the UI (refresh engine exists but deferred to Stage 3)
- Modify DR from the UI (modification engine exists but deferred to Stage 3)
- Multi-user approval workflow (single user reviews and approves their own scans)
- Real-time WebSocket progress updates (polling is sufficient for v1)
