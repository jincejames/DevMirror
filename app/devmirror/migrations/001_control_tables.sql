-- DevMirror control tables DDL
-- Apply with: devmirror.control.control_table.apply_control_ddl(sql_executor, settings)
--
-- All tables live in {control_catalog}.{control_schema} (a single admin schema).
-- See: lakeforge-specs/001-devmirror-dr-cloning-lifecycle/data-model.md
--      SPECIFICATION.md section 3.5
--
-- Placeholders {control_catalog} and {control_schema} are substituted at runtime.

-- 1. Development Requests
CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.devmirror_development_requests (
    dr_id                STRING      NOT NULL,
    description          STRING,
    status               STRING      NOT NULL,
    config_yaml          STRING,
    created_at           TIMESTAMP   NOT NULL,
    created_by           STRING      NOT NULL,
    expiration_date      DATE        NOT NULL,
    last_refreshed_at    TIMESTAMP,
    last_modified_at     TIMESTAMP,
    notification_sent_at TIMESTAMP
);

-- 2. DR Objects
CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.devmirror_dr_objects (
    dr_id                STRING      NOT NULL,
    source_fqn           STRING      NOT NULL,
    target_fqn           STRING      NOT NULL,
    target_environment   STRING      NOT NULL,
    object_type          STRING      NOT NULL,
    access_mode          STRING      NOT NULL,
    clone_strategy       STRING      NOT NULL,
    clone_revision_mode  STRING      NOT NULL,
    clone_revision_value STRING,
    provisioned_at       TIMESTAMP,
    last_refreshed_at    TIMESTAMP,
    status               STRING      NOT NULL,
    estimated_size_gb    DOUBLE
);

-- 3. DR Access grants
CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.devmirror_dr_access (
    dr_id          STRING      NOT NULL,
    user_email     STRING      NOT NULL,
    environment    STRING      NOT NULL,
    access_level   STRING      NOT NULL,
    granted_at     TIMESTAMP   NOT NULL
);

-- 4. Audit log (append-only)
CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.audit_log (
    log_id         STRING      NOT NULL,
    dr_id          STRING      NOT NULL,
    action         STRING      NOT NULL,
    action_detail  STRING,
    performed_by   STRING      NOT NULL,
    performed_at   TIMESTAMP   NOT NULL,
    status         STRING      NOT NULL,
    error_message  STRING
);
