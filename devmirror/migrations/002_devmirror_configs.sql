-- DevMirror UI config storage table
-- Stores user-created configs from the web app before they become full DRs.
-- Placeholders {control_catalog} and {control_schema} are substituted at runtime.

CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.fastsetup_configs (
    dr_id STRING,
    config_json STRING,
    config_yaml STRING,
    status STRING,
    validation_errors STRING,
    created_at STRING,
    created_by STRING,
    updated_at STRING,
    expiration_date STRING,
    description STRING,
    manifest_json STRING,
    scanned_at STRING,
    rejection_comment STRING,
    rejected_by STRING,
    rejected_at STRING
);

-- Forward-compat: in-place migration for deployments whose
-- fastsetup_configs was created before the rejection columns existed.
-- ALTER raises on a second run because Databricks SQL doesn't support
-- `ADD COLUMNS IF NOT EXISTS`; `apply_control_ddl` swallows the
-- per-statement error and keeps going.
ALTER TABLE {control_catalog}.{control_schema}.fastsetup_configs
  ADD COLUMNS (rejection_comment STRING, rejected_by STRING, rejected_at STRING);
