# DevMirror App UI: Configuration & Validation

## Specification v1.0 -- Stage 1

---

## 1. Overview

A Databricks App (APX: FastAPI backend + React frontend) providing a web UI for DevMirror. Stage 1 covers configuration input, storage, and validation only -- no scan, provision, or lifecycle operations.

Users fill out a form to create or edit a Development Request (DR), the app validates it against the same rules as the CLI, and stores the validated config for later use by the DevMirror engine.

### 1.1 Why an App

- Developers shouldn't need CLI access or YAML knowledge to submit a DR
- Form validation catches errors before submission
- Centralized config storage replaces scattered YAML files
- Foundation for Stage 2 (scan/provision/monitor from the UI)

### 1.2 Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | FastAPI (Python), Databricks App runtime |
| Frontend | React (TypeScript), Databricks APX scaffold |
| Auth | Databricks App OAuth (automatic user identity) |
| Storage | Unity Catalog Delta table (`devmirror_configs`) + optional UC Volume for YAML export |
| Validation | DevMirror `config/schema.py` Pydantic models (reused directly) |
| SQL Execution | SQL warehouse via `DEVMIRROR_WAREHOUSE_ID` (Apps have no SparkSession) |

---

## 2. Backend API

The FastAPI backend imports DevMirror's existing Pydantic models and validation functions directly -- no duplication.

### 2.1 Endpoints

#### `POST /api/configs`

Create a new DR configuration.

**Request body:**
```json
{
  "dr_id": "DR-1042",
  "description": "Refactor customer churn pipeline",
  "streams": ["customer_churn_daily", "customer_segmentation_weekly"],
  "additional_objects": ["prod_analytics.marketing.campaign_dim"],
  "qa_enabled": false,
  "data_revision_mode": "latest",
  "data_revision_version": null,
  "data_revision_timestamp": null,
  "developers": ["dev1@company.com", "dev2@company.com"],
  "qa_users": [],
  "expiration_date": "2026-06-15",
  "notification_days_before": 7,
  "notification_recipients": ["dev1@company.com"]
}
```

**Response (201):**
```json
{
  "dr_id": "DR-1042",
  "status": "valid",
  "created_at": "2026-04-14T10:00:00Z",
  "created_by": "dev1@company.com",
  "config": { ... }
}
```

**Response (422):**
```json
{
  "dr_id": "DR-1042",
  "status": "invalid",
  "errors": [
    {"field": "expiration_date", "message": "Must be in the future."},
    {"field": "streams", "message": "At least one stream is required."}
  ]
}
```

#### `PUT /api/configs/{dr_id}`

Update an existing configuration. Same body as POST. Overwrites the stored config.

#### `GET /api/configs/{dr_id}`

Retrieve a stored configuration.

**Response (200):**
```json
{
  "dr_id": "DR-1042",
  "description": "Refactor customer churn pipeline",
  "streams": ["customer_churn_daily"],
  "status": "valid",
  "created_at": "2026-04-14T10:00:00Z",
  "created_by": "dev1@company.com",
  "config_yaml": "version: \"1.0\"\ndevelopment_request:\n  ...",
  "validation_errors": []
}
```

#### `GET /api/configs`

List all stored configurations for the current user (or all if admin).

**Response (200):**
```json
{
  "configs": [
    {"dr_id": "DR-1042", "description": "...", "status": "valid", "created_at": "...", "expiration_date": "2026-06-15"},
    {"dr_id": "DR-1043", "description": "...", "status": "invalid", "created_at": "...", "expiration_date": "2026-07-01"}
  ]
}
```

#### `DELETE /api/configs/{dr_id}`

Delete a stored configuration (only if not yet provisioned).

#### `POST /api/configs/{dr_id}/validate`

Re-validate a stored config against current policy rules (expiration may have become stale).

**Response (200):**
```json
{
  "dr_id": "DR-1042",
  "valid": true,
  "errors": []
}
```

#### `GET /api/configs/{dr_id}/yaml`

Export the stored config as a YAML file download (for use with the CLI).

#### `GET /api/streams/search?q={query}`

Search for available Databricks workflows and pipelines by name (typeahead for the streams field).

**Response (200):**
```json
{
  "results": [
    {"name": "customer_churn_daily", "type": "job", "id": "123456"},
    {"name": "customer_segmentation_weekly", "type": "pipeline", "id": "789012"}
  ]
}
```

### 2.2 Storage

**Table: `{control_catalog}.{control_schema}.devmirror_configs`**

| Column | Type | Description |
|--------|------|-------------|
| `dr_id` | STRING | Primary key. DR identifier. |
| `config_json` | STRING | Full config as JSON (from Pydantic `model_dump_json()`). |
| `config_yaml` | STRING | Full config as YAML (for export/display). |
| `status` | STRING | `valid` / `invalid` / `provisioned` |
| `validation_errors` | STRING | JSON array of error strings (empty if valid). |
| `created_at` | TIMESTAMP | When first saved. |
| `created_by` | STRING | User who created (from OAuth identity). |
| `updated_at` | TIMESTAMP | Last update. |
| `expiration_date` | DATE | Denormalized for list views. |
| `description` | STRING | Denormalized for list views. |

This table is separate from the existing `devmirror_development_requests` control table. Configs are stored here during the form/validation phase. When a user later triggers provisioning (Stage 2), the config moves to the control table.

### 2.3 Validation Logic

The backend reuses DevMirror's existing validation stack directly:

```python
from devmirror.config.schema import DevMirrorConfig, DevMirrorConfigError
from devmirror.utils.validation import validate_config_for_submission

# 1. Schema validation (Pydantic)
try:
    config = DevMirrorConfig.model_validate(form_data_as_dict)
except ValidationError as e:
    return {"status": "invalid", "errors": format_pydantic_errors(e)}

# 2. Policy validation (expiration, QA users, etc.)
policy_errors = validate_config_for_submission(config)
if policy_errors:
    return {"status": "invalid", "errors": policy_errors}

return {"status": "valid"}
```

No validation logic is duplicated between frontend and backend. The frontend does basic UX checks (required fields, format hints) but the backend is the source of truth.

---

## 3. Frontend

### 3.1 Pages

#### Config List (`/`)

A table showing all saved DR configs with columns: DR ID, Description, Status (valid/invalid), Expiration Date, Created By, Created At. Actions: Edit, Delete, Export YAML.

#### Config Form (`/config/new` and `/config/{dr_id}`)

A multi-section form for creating or editing a DR config. Sections map to the YAML structure:

**Section 1: Basic Info**
- DR ID (text input, pattern `DR-<digits>`, auto-generated if blank)
- Description (text area, optional)

**Section 2: Streams**
- Streams list (typeahead search against `/api/streams/search`, add/remove)
- Additional objects (multi-line input, one FQN per line, optional)

**Section 3: Environments**
- Dev (always enabled, shown as read-only checkbox)
- QA (toggle, optional)

**Section 4: Data Revision**
- Mode selector (radio: Latest / Specific Version / Specific Timestamp)
- Version input (number, shown when mode=version)
- Timestamp input (datetime picker, shown when mode=timestamp)

**Section 5: Access**
- Developers (multi-input, at least 1, supports user/group names)
- QA Users (multi-input, shown when QA enabled)

**Section 6: Lifecycle**
- Expiration date (date picker, must be future, max 90 days shown as hint)
- Notification days before (number, default 7)
- Notification recipients (multi-input, defaults to developers list)

**Actions:**
- **Save Draft** -- stores config without validation
- **Validate & Save** -- runs full validation, stores with status
- **Export YAML** -- downloads the config as a YAML file

### 3.2 UX Behavior

- **Inline hints**: Each field shows a brief helper text (e.g., "Three-part name: catalog.schema.table")
- **Live feedback**: Required field indicators, format validation on blur (DR ID pattern, date format)
- **Validation results**: After "Validate & Save", errors are shown inline next to the relevant fields with red highlights
- **Stream typeahead**: Debounced search (300ms) against the workspace job/pipeline list
- **Auto-populate notification recipients**: When developers list changes and recipients is empty, auto-fill from developers

### 3.3 Component Structure

```
src/
  App.tsx                  -- Router: / -> ConfigList, /config/:id -> ConfigForm
  api/
    client.ts              -- API client for /api/configs and /api/streams
  pages/
    ConfigList.tsx          -- Table view of all configs
    ConfigForm.tsx          -- Create/edit form
  components/
    StreamSearch.tsx         -- Typeahead stream search
    MultiInput.tsx           -- Reusable multi-value text input
    RevisionSelector.tsx     -- Mode radio + conditional inputs
    ValidationErrors.tsx     -- Error display panel
```

---

## 4. App Configuration

### 4.1 `app.yaml`

```yaml
command:
  - uvicorn
  - app.main:app
  - --host=0.0.0.0
  - --port=8000

env:
  - name: DEVMIRROR_WAREHOUSE_ID
    description: SQL warehouse for config storage queries
  - name: DEVMIRROR_CONTROL_CATALOG
    description: Catalog for DevMirror tables
  - name: DEVMIRROR_CONTROL_SCHEMA
    description: Schema for DevMirror tables

resources:
  - name: devmirror-warehouse
    sql_warehouse:
      id: ${DEVMIRROR_WAREHOUSE_ID}
      permission: CAN_USE
```

### 4.2 Authentication

Databricks Apps provide OAuth tokens automatically. The backend extracts the user identity from the request headers:

```python
def get_current_user(request: Request) -> str:
    return request.headers.get("X-Forwarded-Email", "unknown")
```

This identifies who created each config for the `created_by` field.

---

## 5. Integration with Existing DevMirror

The app imports from the `devmirror` package directly (installed as a dependency):

| App needs | DevMirror provides |
|-----------|-------------------|
| Config validation | `DevMirrorConfig.model_validate()` |
| Policy validation | `validate_config_for_submission()` |
| YAML generation | `yaml.safe_dump(config.model_dump())` |
| Stream search | `WorkspaceClient().jobs.list()` / `pipelines.list_pipelines()` |
| Config storage SQL | `DbClient.sql()` / `DbClient.sql_exec()` |

No DevMirror code is duplicated or forked. The app is a thin UI layer over the existing library.

---

## 6. Out of Scope (Stage 1)

- Triggering scan/provision/refresh/modify/cleanup from the UI (Stage 2)
- Monitoring DR status and object state (Stage 2)
- Viewing audit logs (Stage 2)
- Approval workflows for manifest review (Stage 2)
- Multi-user access control beyond OAuth identity (Stage 2)
