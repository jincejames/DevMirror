# DevMirror: Production-to-Development Object Cloning Utility

## Specification v1.0

---

## 1. Overview

DevMirror is a Databricks-native utility that automates the cloning and lifecycle management of production Unity Catalog objects (schemas, tables, views) into an isolated development environment. Given a configuration describing a development request (DR), DevMirror scans production ETL streams (Databricks Workflows / Lakeflow Pipelines), resolves all objects read or created by the streams, and provisions scoped, isolated replicas in the development catalog — enabling developers to test against real production data without interfering with other developers or production workloads.

### 1.1 Key Terms


| Term                         | Definition                                                                                                                                                               |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **DR (Development Request)** | A work request that defines the scope, duration, assigned users, and target streams for a development/testing effort. Each DR has a unique identifier (e.g., `DR-1042`). |
| **Stream**                   | A Databricks ETL Workflow or Lakeflow Declarative Pipeline that reads from source objects and writes to target objects within Unity Catalog.                             |
| **Object**                   | A Unity Catalog entity: catalog, schema, table, or view.                                                                                                                 |
| **PROD Environment**         | The production Unity Catalog catalog(s) and associated storage accounts containing authoritative data.                                                                   |
| **DEV Environment**          | The development Unity Catalog catalog(s) and associated storage accounts where cloned objects are provisioned.                                                           |
| **Revision**                 | A point-in-time snapshot of data, either a Delta table version number or a timestamp-based state (time travel).                                                          |


### 1.2 Architecture Summary (from design)

```
                          +---------------------------+
                          | System Lineage Table      |
                          | (object dependency graph) |
                          +------------+--------------+
                                       |
  +----------------+        Scan       v
  | User Input     | -----> [Scan Engine] ---reads---> [PROD Environment]
  | (Config/YAML)  |            |                       Sub Domain Catalog
  |  - stream_name |            v                         Schema
  |  - dr_id       |     [Human Review]                     Table/View
  |  - user_list   |            |                         Storage Account
  |  - qa_list     |            v
  |  - object_list |     [DevMirror Core Utility]
  |  - time_limit  |        |         |         |
  |  - qa_version  |        |         |         |
  |  - version     |        v         v         v
  +----------------+   [Control   [Clone/    [Cleanup
                        Table]    Write/     Jobs]
                          |       Replace]      |
  +----------------+      |         |           |
  | Modifications  |------+         v           v
  | (update DR)    |        [DEV Environment]
  +----------------+         Sub Domain Catalog
                               Schema (dr_XXX_ / qa_XXX_ prefix)
                                 Table/View
                               Storage Account
                                 |
                                 +---> ETL Cluster (SELECT, WRITE)
                                 +---> User Access (R/W scoped)
```

---

## 2. User Input Configuration

DevMirror accepts a YAML configuration file (or future UI form) that defines a single development request.

### 2.1 Configuration Schema

```yaml
# devmirror-config.yaml
version: "1.0"

development_request:
  dr_id: "DR-1042"                       # Unique development request identifier (required)
  description: "Refactor customer churn pipeline"  # Human-readable description (optional)

  streams:
    - name: "customer_churn_daily"       # Fully qualified Databricks workflow/pipeline name (required)
    - name: "customer_segmentation_weekly"

  additional_objects:                     # Extra objects not discovered by stream scan (optional)
    - "prod_analytics.marketing.campaign_dim"
    - "prod_analytics.shared.date_dim"

  environments:
    dev:
      enabled: true                      # Always true; primary dev environment (required)
    qa:
      enabled: true                      # Enable QA environment (optional, default: false)

  data_revision:
    mode: "latest"                       # One of: "latest", "version", "timestamp" (required)
    # version: 42                        # Delta table version number (required if mode=version)
    # timestamp: "2026-04-01T00:00:00Z"  # ISO 8601 timestamp (required if mode=timestamp)

  access:
    developers:                          # Users with read/write access to dev environment (required, min 1)
      - user-group-name # Can be a user group
      - "jince.james@company.com"
      - "dev.user2@company.com"
    qa_users:                            # or individual Users with read/write access to qa environment (optional)
      - "qa.lead@company.com"

  lifecycle:
    expiration_date: "2026-06-15"        # ISO 8601 date when the DR expires and cleanup triggers (required)
    notification_days_before: 7          # Days before expiration to send email notification (default: 7)
    notification_recipients:             # Email recipients for lifecycle notifications (defaults to developers + qa_users), defaults to developer list above
      - "jince.james@company.com"
      - "team-lead@company.com"
```

### 2.2 Configuration Validation Rules


| Field                     | Rule                                                                                |
| ------------------------- | ----------------------------------------------------------------------------------- |
| `dr_id`                   | can be existing, but exists should overwrite the current request aka treat it as modification Format: `DR-<digits>`.                        |
| `streams[].name`          | Must resolve to an existing Databricks Workflow or Pipeline in PROD.                |
| `data_revision.timestamp` | Must be within the Delta table retention window for all objects in scope.           |
| `expiration_date`         | Must be a future date. Maximum allowed duration is configurable (default: 90 days). |
| `developers`              | At least one developer must be specified.                                           |


---

## 3. Functional Components

### 3.1 Scan module

**Purpose:** Given one or more stream names, automatically discover all Unity Catalog objects (tables, views, schemas) required to run those streams in isolation on DEV.

#### 3.1.1 Scan Process

1. **Workflow/Pipeline Resolution**: For each stream name, resolve it to a Databricks Workflow (job) or Lakeflow Declarative Pipeline definition. Extract all task definitions (notebooks, Python files, SQL files, pipeline declarations).
2. **Lineage Extraction**: Query the Unity Catalog system lineage tables (`system.access.table_lineage`, `system.access.column_lineage`) to build a dependency graph of all objects read and written by the stream.
3. **System Lineage Table Enrichment**: optionally Cross-reference with a maintained system lineage/image table (a curated metadata table storing known object associations per stream if exists) to catch dependencies not captured by automatic lineage — e.g., dynamic SQL references, configuration-driven table names, or external lookup tables.
4. **Dependency Classification**: For each discovered object, classify it as:
  - **READ-ONLY**: The stream only reads from this object (SELECT). 
  - **READ-WRITE**: The stream both reads and writes to this object. 
  - **WRITE-ONLY**: The stream creates or fully overwrites this object.
  - All classification use shallow clones.
5. **Object List Generation**: Produce a complete manifest of objects with their classifications, organized by schema.

#### 3.1.2 Scan Output (Object Manifest)

The scan produces a manifest stored in the control table and also optionally serializable as YAML for human review:

```yaml
# Auto-generated by DevMirror Scan Engine
scan_result:
  dr_id: "DR-1042"
  scanned_at: "2026-04-13T10:30:00Z"
  streams_scanned:
    - name: "customer_churn_daily"
      workflow_id: "1234567890"
      tasks: ["ingest_raw", "transform_silver", "aggregate_gold"]

  objects:
    - fqn: "prod_analytics.customers.customer_profile"
      type: "table"
      format: "delta"
      access_mode: "READ_ONLY"
        # view | deep_clone | shallow_clone | schema_only
      estimated_size_gb: 45.2

    - fqn: "prod_analytics.customers.churn_scores"
      type: "table"
      format: "delta"
      access_mode: "READ_WRITE"
      estimated_size_gb: 2.1

    - fqn: "prod_analytics.customers.churn_daily_output"
      type: "table"
      format: "delta"
      access_mode: "WRITE_ONLY"
      estimated_size_gb: 0

    - fqn: "prod_analytics.shared.date_dim"
      type: "view"
      access_mode: "READ_ONLY"
      estimated_size_gb: 0

  schemas_required:
    - "prod_analytics.customers"
    - "prod_analytics.shared"

  total_objects: 4
  review_required: true               # Set true when system lineage table had gaps
```

#### 3.1.3 Human Review

After scan completes, the manifest is presented for human review before provisioning proceeds. The reviewer can:

- Add objects missed by the scan.
- Remove objects not needed.
- Override clone strategies (e.g., force deep clone instead of view for a read-only table if the developer needs to test mutations).
- Approve or reject the manifest.

The review step is mandatory on first provisioning and optional on subsequent refreshes.

---

### 3.2 Provisioning module

**Purpose:** Given an approved object manifest and configuration, create all required objects in the DEV environment with proper isolation and access controls.

#### 3.2.1 Schema Provisioning

For each schema discovered in the scan:

1. Create a prefixed schema in the respective catalog (can be identified by source catalog with suffix):
  - DEV: `{catalog_dev_env_suffix}.{prefix}_{original_schema_name}`
  - QA (if enabled): `{catalog_qa_env_suffix}.{qa_prefix}_{original_schema_name}`
   Example:
  - PROD: `prod_analytics.customers`
  - DEV: `dev_analytics.dr_1042_customers`
  - QA: `dev_analytics.qa_1042_customers`
2. Apply schema-level grants to the specified developers/qa_users.

#### 3.2.2 Object Cloning Strategies


| Strategy          | When Used                              | SQL Pattern                                                                                                   | Storage Impact                                  |
| ----------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| **View**          |                  | `CREATE VIEW {dev_schema}.{table} AS SELECT * FROM {prod_schema}.{table}`                                     | Zero (reads PROD directly)                      |
| **Deep Clone**    |                       | `CREATE TABLE {dev_schema}.{table} DEEP CLONE {prod_schema}.{table} [VERSION AS OF N | TIMESTAMP AS OF T]`    | Full copy of data                               |
| **Shallow Clone** |  | `CREATE TABLE {dev_schema}.{table} SHALLOW CLONE {prod_schema}.{table} [VERSION AS OF N | TIMESTAMP AS OF T]` | Metadata only; shares data files until modified |
| **Schema Only**   |   | `CREATE TABLE {dev_schema}.{table} LIKE {prod_schema}.{table}`                                                | Empty table, schema only                        |


**Revision support:** When `data_revision.mode` is `version` or `timestamp`, all deep/shallow clones use the `VERSION AS OF` or `TIMESTAMP AS OF` clause respectively. Views for read-only objects also incorporate the revision:

```sql
CREATE VIEW {dev_schema}.{table} AS
  SELECT * FROM {prod_schema}.{table} VERSION AS OF {version}
```
But when it is not mentioned, it should default to latest version

#### 3.2.3 Access Control Provisioning

For each provisioned schema:

```sql
-- Grant developers access to dev schemas
GRANT USAGE ON SCHEMA {dev_catalog}.{dr_prefix}_{schema} TO `developer@company.com`;
GRANT SELECT, MODIFY ON SCHEMA {dev_catalog}.{dr_prefix}_{schema} TO `developer@company.com`;

-- Grant QA users access to qa schemas (if QA enabled)
GRANT USAGE ON SCHEMA {dev_catalog}.{qa_prefix}_{schema} TO `qa_user@company.com`;
GRANT SELECT, MODIFY ON SCHEMA {dev_catalog}.{qa_prefix}_{schema} TO `qa_user@company.com`;

---

### 3.3 Refresh Module

**Purpose:** Allow developers to re-sync DEV objects from PROD at any time, either to the latest version or a specified historical revision.                                                                                                                  |


#### 3.3.1 Refresh Execution

1. Validate the DR is still active (not expired).
2. Validate the requested revision is within Delta retention.
3. For each object in scope:
  - **Views**: Recreate with updated revision clause (if revision changed), or no-op if `latest`.
  - **Deep/Shallow Clones**: Execute `CREATE OR REPLACE TABLE ... DEEP/SHALLOW CLONE ... [VERSION/TIMESTAMP]`.
  - **Schema-Only tables**: Option to truncate or leave as-is 
4. Update control table with refresh timestamp and revision info.

---

### 3.4 Modification Module

**Purpose:** Enable changes to an already-submitted and active development request without requiring a full re-provisioning.

#### 3.4.1 Supported Modifications


| Modification               | Action                                                                                            |
| -------------------------- | ------------------------------------------------------------------------------------------------- |
| **Add objects**            | Run scan for new objects or accept manual additions. Provision only the new objects.              |
| **Remove objects**         | Drop the specified objects from DEV/QA schemas. Revoke associated grants.                         |
| **Add schemas**            | Create new prefixed schemas and provision objects within them.                                    |
| **Remove schemas**         | Drop prefixed schemas and all contained objects. Revoke grants.                                   |
| **Change expiration date** | Update the expiration in the control table. Recalculate notification schedule.                    |
| **Add/remove users**       | Grant/revoke access on all schemas and objects in the DR scope.                                   |
| **Add streams**            | Run scan for the new stream(s), merge the object manifest with the existing one, provision delta. |


#### 3.4.2 Modification Configuration

Follows the same as original input configuration

#### 3.4.3 Modification Execution

1. Load the current DR state from the control table.
2. Validate all modifications (e.g., new objects exist in PROD, new users are valid, new expiration is valid).
3. Execute each action atomically where possible. If an action fails, log the failure and continue with remaining actions (partial success model).
4. Update the control table with the new DR state.
5. Generate a modification audit log entry.

---

### 3.5 Control Table

**Purpose:** Central metadata store tracking all active DRs, their objects, configurations, and lifecycle state.

#### 3.5.1 Schema

The control table resides in a dedicated DevMirror management schema: `{dev_catalog}.utilities`.

**Table: `devmirror_development_requests`**


| Column                 | Type      | Description                                                                                                                  |
| ---------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `dr_id`                | STRING    | Primary key. Unique DR identifier.                                                                                           |
| `description`          | STRING    | Human-readable description.                                                                                                  |
| `status`               | STRING    | `PENDING_REVIEW` / `PROVISIONING` / `ACTIVE` / `EXPIRING_SOON` / `EXPIRED` / `CLEANUP_IN_PROGRESS` / `CLEANED_UP` / `FAILED` |
| `config_yaml`          | STRING    | Full YAML configuration as submitted.                                                                                        |
| `created_at`           | TIMESTAMP | DR creation timestamp.                                                                                                       |
| `created_by`           | STRING    | User who created the DR.                                                                                                     |
| `expiration_date`      | DATE      | When the DR expires.                                                                                                         |
| `last_refreshed_at`    | TIMESTAMP | Last data refresh timestamp.                                                                                                 |
| `last_modified_at`     | TIMESTAMP | Last modification timestamp.                                                                                                 |
| `notification_sent_at` | TIMESTAMP | When the pre-expiry notification was sent (NULL if not yet).                                                                 |


**Table: `devmirror_dr_objects`**


| Column                 | Type      | Description                                              |
| ---------------------- | --------- | -------------------------------------------------------- |
| `dr_id`                | STRING    | FK to `development_requests`.                            |
| `source_fqn`           | STRING    | Fully qualified name of the PROD object.                 |
| `target_fqn`           | STRING    | Fully qualified name of the DEV object.                  |
| `target_environment`   | STRING    | `dev` or `qa`.                                           |
| `object_type`          | STRING    | `table` / `view` / `schema`                              |
| `access_mode`          | STRING    | `READ_ONLY` / `READ_WRITE` / `WRITE_ONLY`                |
| `clone_strategy`       | STRING    | `view` / `deep_clone` / `shallow_clone` / `schema_only`  |
| `clone_revision_mode`  | STRING    | `latest` / `version` / `timestamp`                       |
| `clone_revision_value` | STRING    | Version number or timestamp string.                      |
| `provisioned_at`       | TIMESTAMP | When the object was cloned/created.                      |
| `last_refreshed_at`    | TIMESTAMP | Last refresh of this specific object.                    |
| `status`               | STRING    | `PROVISIONED` / `REFRESH_PENDING` / `FAILED` / `DROPPED` |
| `estimated_size_gb`    | DOUBLE    | Estimated storage size.                                  |


**Table: `devmirror_dr_access`**


| Column         | Type      | Description                                         |
| -------------- | --------- | --------------------------------------------------- |
| `dr_id`        | STRING    | FK to `development_requests`.                       |
| `user_email`   | STRING    | User principal.                                     |
| `environment`  | STRING    | `dev` or `qa`.                                      |
| `access_level` | STRING    | `READ_WRITE` (developers) / `READ_ONLY` (optional). |
| `granted_at`   | TIMESTAMP | When access was granted.                            |

                        |


**Table: `devmirror_admin.audit_log`**


| Column          | Type      | Description                                                          |
| --------------- | --------- | -------------------------------------------------------------------- |
| `log_id`        | STRING    | Unique log entry ID.                                                 |
| `dr_id`         | STRING    | FK to `development_requests`.                                        |
| `action`        | STRING    | `CREATE` / `PROVISION` / `REFRESH` / `MODIFY` / `CLEANUP` / `NOTIFY` |
| `action_detail` | STRING    | JSON detail of what was done.                                        |
| `performed_by`  | STRING    | User or `SYSTEM`.                                                    |
| `performed_at`  | TIMESTAMP | Timestamp.                                                           |
| `status`        | STRING    | `SUCCESS` / `PARTIAL_SUCCESS` / `FAILED`                             |
| `error_message` | STRING    | Error details if failed.                                             |


---

### 3.6 Cleanup Engine

**Purpose:** Automatically remove all DEV objects, schemas, access grants, and cloned workflows when a DR expires.

#### 3.6.1 Cleanup Trigger

A scheduled Databricks Workflow runs daily and:

1. Queries `development_requests` for rows where `expiration_date <= CURRENT_DATE()` and `status = 'ACTIVE'`.
2. For each expired DR, executes the cleanup process.

#### 3.6.2 Pre-Expiry Notification

A separate daily job (or task within the same workflow):

1. Queries for DRs where `expiration_date - notification_days_before <= CURRENT_DATE()` and `notification_sent_at IS NULL` and `status = 'ACTIVE'`.
2. Sends email notifications to all configured recipients listing:
  - DR ID and description.
  - Expiration date.
  - Number of objects that will be cleaned up.
  - Instructions to extend the DR (modification workflow).

Email delivery uses Databricks notification destinations or a configured SMTP endpoint.

#### 3.6.3 Cleanup Process

1. Set DR status to `CLEANUP_IN_PROGRESS`.
2. Drop all cloned workflows/pipelines (DEV and QA).
3. Revoke all grants issued for this DR.
4. For each object in `dr_objects` (ordered: tables/views first, then schemas):
  - `DROP VIEW IF EXISTS {target_fqn}`
  - `DROP TABLE IF EXISTS {target_fqn}`
5. Drop all prefixed schemas:
  - `DROP SCHEMA IF EXISTS {target_catalog}.{prefix}_{schema} CASCADE`
6. Set DR status to `CLEANED_UP`.
7. Log all actions to `audit_log`.

---

## 4. Isolation Model

### 4.1 Schema-Prefix Isolation

Each DR gets uniquely prefixed schemas, ensuring complete isolation between developers:

```
analytics_dev.dr_1042_customers    -- Developer A's work
analytics_dev.dr_1043_customers    -- Developer B's work (same source schema, isolated)
analytics_qa.qa_dr_1042_customers    -- Developer A's QA environment
```

### 4.2 Rules

1. A developer can only see and access schemas/objects associated with their DR (enforced by Unity Catalog grants).
2. Maximum **2 environments per DR**: one  prefixed with work request id (development) and one `qa_` prefixed with work request id (testing/regression).
3. No arbitrary environment names are allowed. Prefixes must conform to `dr_<dr_number>` and `qa_<dr_number>`.
4. Cross-DR data access does not happen by default. If a developer explicitly needs data from another DR, it must be implemented at the application level (e.g., referencing another DR's schema directly in code).


## 5. Implementation Plan

### 5.1 Technology Stack


| Component               | Technology                                                  |
| ----------------------- | ----------------------------------------------------------- |
| **Core Logic**          | Python (Databricks notebook )                |
| **Configuration**       | YAML files parsed with `pyyaml` or `strictyaml`             |
| **Metadata Queries**    | Databricks SQL via `databricks-sdk` or Spark SQL            |
| **Lineage Queries**     | Unity Catalog system tables python SDK (`system.access.table_lineage`) |
| **Workflow Management** | Databricks SDK (`WorkflowsAPI`, `PipelinesAPI`)             |
| **Access Control**      | Unity Catalog SQL GRANT/REVOKE statements                   |
| **Scheduling**          | Databricks Workflows (for cleanup and notification jobs)    |
| **Email Notifications** | Databricks notification destinations or SMTP integration    |
| **CLI Interface**       | Python CLI (argparse/click) or Databricks notebook widgets  |


### 5.2 Module Structure

```
devmirror/
  __init__.py
  cli.py                    # CLI entry point (scan, provision, refresh, modify, cleanup)
  config/
    __init__.py
    schema.py               # YAML config schema validation (pydantic models)
    loader.py               # Config file loading and parsing
  scan/
    __init__.py
    lineage.py              # Unity Catalog lineage queries
    stream_resolver.py      # Resolve stream names to workflow/pipeline definitions
    dependency_classifier.py # Classify objects as READ_ONLY, READ_WRITE, WRITE_ONLY
    manifest.py             # Generate and serialize object manifest
  provision/
    __init__.py
    schema_provisioner.py   # Create prefixed schemas in DEV
    object_cloner.py        # Execute clone strategies (view, deep, shallow, schema_only)
    access_manager.py       # Grant/revoke Unity Catalog permissions
    stream_cloner.py        # Clone and rewrite workflow/pipeline definitions
  refresh/
    __init__.py
    refresh_engine.py       # Full, incremental, and selective refresh logic
  modify/
    __init__.py
    modification_engine.py  # Process DR modification requests
  cleanup/
    __init__.py
    cleanup_engine.py       # Drop objects, revoke grants, clean up workflows
    notifier.py             # Pre-expiry email notification logic
  control/
    __init__.py
    control_table.py        # CRUD operations on the control/metadata tables
    audit.py                # Audit log operations
  utils/
    __init__.py
    sql_executor.py         # Execute SQL against Databricks warehouse
    naming.py               # Schema/object naming conventions and prefix generation
    validation.py           # Cross-cutting validation utilities
```

### 5.3 CLI Interface

```bash
# Scan streams and generate object manifest
devmirror scan --config devmirror-config.yaml --output manifest.yaml

# Provision DEV environment from approved manifest
devmirror provision --config devmirror-config.yaml --manifest manifest.yaml

# Provision directly (scan + auto-approve + provision in one step)
devmirror provision --config devmirror-config.yaml --auto-approve

# Refresh DEV data from PROD
devmirror refresh --config devmirror-refresh.yaml
# Or inline:
devmirror refresh --dr-id DR-1042 --mode incremental --revision latest

# Modify an active DR
devmirror modify --config devmirror-modify.yaml

# Manual cleanup (normally automated)
devmirror cleanup --dr-id DR-1042

# Show status of a DR
devmirror status --dr-id DR-1042

# List all active DRs
devmirror list
```

---

## 6. Workflows and Sequence Flows

### 6.1 Initial Provisioning Flow

```
User submits config YAML
        |
        v
[1. Validate Config]
  - Validate stream names exist in PROD
  - Validate prefix patterns and no collisions
  - Validate users exist
        |
        v
[2. Scan Streams]
  - Resolve stream -> workflow/pipeline
  - Query lineage tables
  - Cross-reference system lineage table
  - Classify objects (READ_ONLY, READ_WRITE, WRITE_ONLY)
  - Generate object manifest
        |
        v
[3. Human Review] (offline, optional)
  - Present manifest for approval
  - Allow additions/removals/overrides
  - Developer approves
        |
        v
[4. Record in Control Table]
  - Insert DR record (status=PROVISIONING)
  - Insert object records
  - Insert access records
  - Insert stream records
        |
        v
[5. Provision Schemas]
  - CREATE SCHEMA for each dr_ and qa_ prefixed schema
        |
        v
[6. Clone Objects] (parallelizable per object)
  - For each object: execute clone strategy SQL
  - Record success/failure per object
        |
        v
[7. Grant Access]
  - GRANT on schemas and objects for all users
  - GRANT SELECT on PROD objects referenced by views
        |
        v

[8. Update Status -> ACTIVE]
  - Log completion in audit table
```

### 6.2 Refresh Flow

```
User submits refresh config or CLI command
        |
        v
[1. Validate]
  - DR exists and is ACTIVE
  - Requested revision is within Delta retention
        |
        v
[2. Determine scope]
  - Full: all objects
  - Incremental: only cloned tables (views auto-refresh)
  - Selective: only specified objects
        |
        v
[3. Execute refresh per object] (parallelizable)
  - Views: recreate if revision changed
  - Deep/Shallow clones: CREATE OR REPLACE ... DEEP CLONE
  - Schema-only: optional truncate
        |
        v
[4. Update control table]
  - Update last_refreshed_at on DR and per-object records
  - Log in audit table
```

### 6.3 Cleanup Flow

```
[Daily Scheduled Job]
        |
        v
[1. Check for expiring DRs]
  - Query: expiration_date - N days <= today AND notification not sent
  - Send email notifications
  - Record notification_sent_at
        |
        v
[2. Check for expired DRs]
  - Query: expiration_date <= today AND status = ACTIVE
        |
        v
[3. For each expired DR:]
  - Set status = CLEANUP_IN_PROGRESS
  - Drop cloned workflows
  - Revoke all grants
  - Drop all objects (views, tables)
  - Drop all prefixed schemas (CASCADE)
  - Set status = CLEANED_UP
  - Log in audit table
```

---

## 7. Error Handling


| Scenario                                             | Behavior                                                                                                                         |
| ---------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- |
| Stream not found in PROD                             | Fail validation with clear error message listing unresolved stream names.                                                        |
| Lineage query returns incomplete results             | Flag in manifest (`review_required: true`), require human review.                                                                |
| Object clone fails (e.g., permissions, table locked) | Log failure, continue with remaining objects, set object status to `FAILED`, set DR status to `ACTIVE` with warnings.            |
| Revision out of Delta retention window               | Fail with error specifying the earliest available version/timestamp per table.                                                   |
| Schema prefix collision                              | Log and replace. Log validation, report which active DR holds the prefix.                                          |
| Cleanup partially fails                              | Log per-object failure, continue cleanup for remaining objects, set DR to `CLEANUP_IN_PROGRESS` for retry on next scheduled run. |
| PROD object dropped while DEV view references it     | View query will fail at runtime. DevMirror does not actively monitor this — developer should re-scan and refresh.                |


---

## 8. Configuration Constants

These are system-level defaults configurable by an administrator in a central DevMirror configuration table or environment variables:

```yaml
devmirror_system_config:
  max_dr_duration_days: 90               # Maximum allowed DR lifetime
  default_notification_days: 7           # Default days before expiry for notification
  shallow_clone_threshold_gb: 50         # Tables above this size use shallow clone for READ_WRITE
  cleanup_schedule_cron: "0 2 * * *"     # Daily at 2 AM
  notification_schedule_cron: "0 8 * * *" # Daily at 8 AM
  control_catalog: "dev_analytics"       # Catalog for DevMirror metadata tables
  control_schema: "devmirror_admin"      # Schema for DevMirror metadata tables
  lineage_system_table: "system.access.table_lineage"
  max_parallel_clones: 10               # Max concurrent clone operations
  audit_retention_days: 365             # How long to keep audit logs
```

---

## 9. Security Considerations

1. **Principle of Least Privilege**: Developers only get access to objects within their DR scope. 
2. **No PROD Write Access**: DevMirror never grants write access to PROD objects. Views are read-only by nature; cloned tables are in DEV storage.
3. **Audit Trail**: All operations (create, refresh, modify, cleanup) are logged in the audit table with user attribution.
4. **Service Principal**: DevMirror itself runs as a service principal with elevated permissions to read PROD metadata/data and write to DEV. Individual developers do not need PROD access beyond what views provide.
5. **Expiration Enforcement**: DRs have a hard maximum lifetime. Extensions require explicit modification actions that are audited.

---

## 10. Out of Scope (for v1)

- UI/Web interface for configuration (future — currently YAML + CLI).
- Automatic monitoring of PROD schema changes and propagation to DEV.
- Cost tracking per DR (can be added by joining with `system.billing.usage`).
- Cross-metastore cloning (assumes PROD and DEV are in the same Unity Catalog metastore).
- Integration with CI/CD pipelines for automated DR provisioning on branch creation.

