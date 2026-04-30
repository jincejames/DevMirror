-- DevMirror DR ID counter table (Stage 4 US-34)
-- Stores the last-allocated counter value per prefix for auto-generated DR IDs.
-- One row per prefix; atomic optimistic-retry UPDATE hands out the next value.
--
-- Placeholders {control_catalog} and {control_schema} are substituted at runtime.

CREATE TABLE IF NOT EXISTS {control_catalog}.{control_schema}.devmirror_id_counter (
    prefix      STRING      NOT NULL,
    last_value  BIGINT      NOT NULL,
    updated_at  TIMESTAMP   NOT NULL
) USING DELTA;
