# Data Model: DevMirror DR cloning lifecycle

**Feature**: `001-devmirror-dr-cloning-lifecycle`  
**Date**: 2026-04-13  
**Normative detail**: `SPECIFICATION.md` sections 2, 3.5, 3.1.2, 8.

## Entity relationship (logical)

```
DevelopmentRequest 1 --- * DrObject
DevelopmentRequest 1 --- * DrAccess
DevelopmentRequest 1 --- * AuditLog
DevelopmentRequest * --- * StreamRef   (logical; store stream names and resolved ids in DR record or child table)
```

Stream-level persistence: SPECIFICATION lists streams inside scan output and config; v1 can store stream list as JSON in `config_yaml` plus optional table `devmirror_dr_streams` if implementation prefers normalization (add during build if query-by-stream is required).

## 1. DevelopmentRequest (`devmirror_development_requests`)

| Field | Type | Validation / notes |
|-------|------|-------------------|
| dr_id | STRING PK | Format `DR-<digits>` per validation rules; upsert = modification of same logical DR |
| description | STRING | Optional |
| status | STRING | Enum: PENDING_REVIEW, PROVISIONING, ACTIVE, EXPIRING_SOON, EXPIRED, CLEANUP_IN_PROGRESS, CLEANED_UP, FAILED |
| config_yaml | STRING | Full submitted YAML |
| created_at | TIMESTAMP | Required |
| created_by | STRING | Required on create |
| expiration_date | DATE | Must be future at create; max duration from system config |
| last_refreshed_at | TIMESTAMP | Nullable |
| last_modified_at | TIMESTAMP | Updated on modify |
| notification_sent_at | TIMESTAMP | Nullable |

**State transitions (primary path)**:

```
PENDING_REVIEW -> PROVISIONING -> ACTIVE -> (EXPIRING_SOON optional label) -> EXPIRED | CLEANUP_IN_PROGRESS -> CLEANED_UP
PROVISIONING | ACTIVE -> FAILED (terminal or recoverable per policy)
CLEANUP_IN_PROGRESS -> CLEANED_UP | CLEANUP_IN_PROGRESS (retry)
```

## 2. DrObject (`devmirror_dr_objects`)

| Field | Type | Validation / notes |
|-------|------|-------------------|
| dr_id | STRING FK | References DevelopmentRequest |
| source_fqn | STRING | Three-part UC name |
| target_fqn | STRING | Three-part UC name in dev or qa catalog |
| target_environment | STRING | `dev` or `qa` |
| object_type | STRING | `table`, `view`, `schema` |
| access_mode | STRING | READ_ONLY, READ_WRITE, WRITE_ONLY |
| clone_strategy | STRING | view, deep_clone, shallow_clone, schema_only |
| clone_revision_mode | STRING | latest, version, timestamp |
| clone_revision_value | STRING | Nullable when mode is latest |
| provisioned_at | TIMESTAMP | |
| last_refreshed_at | TIMESTAMP | |
| status | STRING | PROVISIONED, REFRESH_PENDING, FAILED, DROPPED |
| estimated_size_gb | DOUBLE | From scan estimate |

**Composite uniqueness (recommended)**: (`dr_id`, `source_fqn`, `target_environment`) to prevent duplicate lines.

## 3. DrAccess (`devmirror_dr_access`)

| Field | Type | Validation / notes |
|-------|------|-------------------|
| dr_id | STRING FK | |
| user_email | STRING | Principal identifier; may represent group where UC supports it |
| environment | STRING | `dev` or `qa` |
| access_level | STRING | READ_WRITE default for developers; READ_ONLY optional |
| granted_at | TIMESTAMP | |

## 4. AuditLog (`audit_log` in admin schema)

| Field | Type | Validation / notes |
|-------|------|-------------------|
| log_id | STRING PK | Unique id per entry |
| dr_id | STRING FK | |
| action | STRING | CREATE, PROVISION, REFRESH, MODIFY, CLEANUP, NOTIFY |
| action_detail | STRING | JSON payload |
| performed_by | STRING | User email or SYSTEM |
| performed_at | TIMESTAMP | |
| status | STRING | SUCCESS, PARTIAL_SUCCESS, FAILED |
| error_message | STRING | Nullable |

## 5. Supporting config entities (in YAML / system table)

### 5.1 DevelopmentRequestConfig (submitted YAML)

Nested under keys per SPECIFICATION 2.1: `development_request` with `dr_id`, `description`, `streams[]`, `additional_objects[]`, `environments.dev|qa`, `data_revision`, `access`, `lifecycle`.

Validation highlights:

- `streams[].name`: must resolve in prod
- `lifecycle.expiration_date`: future date; within `max_dr_duration_days`
- `access.developers`: min length 1
- `data_revision.mode`: latest | version | timestamp with conditional fields

### 5.2 DevmirrorSystemConfig (administrator defaults, SPECIFICATION section 8)

Includes `max_dr_duration_days`, `default_notification_days`, `shallow_clone_threshold_gb`, cron strings, `control_catalog`, `control_schema`, `lineage_system_table`, `max_parallel_clones`, `audit_retention_days`.

## 6. Scan manifest (pre-control or exported YAML)

Logical entity `ScanResult`: `dr_id`, `scanned_at`, `streams_scanned[]`, `objects[]` (fqn, type, format, access_mode, estimated_size_gb), `schemas_required[]`, `total_objects`, `review_required`.

Maps to DrObject proposals before approval; not necessarily persisted as a separate table if manifest is folded into `config_yaml` and DrObject rows at provision time.
