-- DevMirror UI config storage table
-- Stores user-created configs from the web app before they become full DRs.
-- Placeholders {control_catalog} and {control_schema} are substituted at runtime.

CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.devmirror_configs (
    dr_id STRING,
    config_json STRING,
    config_yaml STRING,
    status STRING,
    validation_errors STRING,
    created_at STRING,
    created_by STRING,
    updated_at STRING,
    expiration_date STRING,
    description STRING
);
