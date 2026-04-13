# CLI command contract (v1)

Normative reference: `SPECIFICATION.md` section 5.3.

All commands exit non-zero on validation failure before side effects. Global flags (workspace profile, warehouse id, verbosity) may be added during implementation but must not change the semantics below.

## `devmirror scan`

- **Input**: `--config` path to development request YAML.
- **Output**: `--output` path to manifest YAML (scan_result shape).
- **Failure modes**: unresolved stream names; unreadable config.

## `devmirror provision`

- **Modes**: (1) `--config` + `--manifest`, or (2) `--config` + `--auto-approve` (runs scan internally, skips external manifest file).
- **Side effects**: writes control tables, creates UC objects, grants access, updates DR status.
- **Failure modes**: prefix collision; missing prod objects; partial object failures recorded per SPECIFICATION 7.

## `devmirror refresh`

- **Input**: `--config` refresh YAML and/or inline `--dr-id`, `--mode`, `--revision`.
- **Side effects**: updates replicas and control timestamps, audit entries.

## `devmirror modify`

- **Input**: `--config` modification YAML (same schema family as create, per SPECIFICATION 3.4.2).
- **Side effects**: incremental provision/drop/grants/date changes with partial success where specified.

## `devmirror cleanup`

- **Input**: `--dr-id` for manual cleanup; scheduled path uses same library code without CLI.

## `devmirror status`

- **Input**: `--dr-id`.
- **Output**: human-readable summary plus optional `--json` for automation (implementation choice; if `--json` is added, document stable keys in tasks phase).

## `devmirror list`

- **Output**: active and pending DRs filtered per default policy.
