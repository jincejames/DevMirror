# Work Packages: DevMirror DR cloning lifecycle

**Inputs**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/` (plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md)  
**Prerequisites**: plan.md, spec.md  
**Tests**: Optional unless a WP explicitly adds pytest files (WP02 includes offline unit tests).

**Organization**: Subtasks `T001`..`T043` roll into work packages `WP01`..`WP08`. Prompt files live flat under `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/`.

## Subtask Format: `[Txxx] [P?] Description`

---

## Work Package WP01: Repository and configuration (Priority: P0)

**Goal**: Installable `devmirror` package with dependency pins, console entrypoint, and validated YAML configuration models aligned with contracts.  
**Independent Test**: `pip install -e .` succeeds locally; loading a sample config from `contracts/` validates or fails with structured errors without touching Databricks.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP01-repository-and-configuration.md`  
**Estimated prompt size**: ~320 lines

### Included Subtasks

- [ ] T001 Add `pyproject.toml` at repo root: Python 3.11+, package metadata, dependencies (`pydantic`, `pyyaml`, `databricks-sdk`, `click`), console script `devmirror = devmirror.cli:main`, optional dev deps `pytest`, `ruff`
- [ ] T002 Create `devmirror/` package skeleton per `plan.md` (empty `__init__.py` files for `config`, `scan`, `provision`, `refresh`, `modify`, `cleanup`, `control`, `utils`)
- [ ] T003 [P] Add minimal `ruff` and `pytest` configuration in `pyproject.toml` (or `ruff.toml`) targeting `devmirror/` and `tests/`
- [ ] T004 Implement `devmirror/config/schema.py` Pydantic models matching `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/config.schema.json` and `SPECIFICATION.md` section 2.1 (including `data_revision` conditionals)
- [ ] T005 Implement `devmirror/config/loader.py` to load YAML from path, surface parse errors with file context

### Implementation Notes

Pin `databricks-sdk` to a current stable range. Keep package importable without DATABRICKS_* env vars set.

### Parallel Opportunities

T003 can proceed alongside T004 if different authors; same file `pyproject.toml` requires merge care -> prefer sequential T001-T002 then [P] T003 vs T004 on different paths if T001-T002 merged first.

### Dependencies

None.

### Risks and mitigations

Dependency resolution on corporate mirrors: document index URL in README in WP08.

---

## Work Package WP02: Runtime utilities and offline tests (Priority: P0)

**Goal**: SQL execution wrapper, naming and validation helpers, environment-backed settings, and fast offline tests.  
**Independent Test**: `pytest tests/unit/test_config_naming_validation.py` passes with no network calls.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP02-runtime-utilities-and-offline-tests.md`  
**Estimated prompt size**: ~380 lines

### Included Subtasks

- [ ] T006 Implement `devmirror/utils/sql_executor.py` using Databricks SQL statement execution API (warehouse id from settings), return typed errors for warehouse failures
- [ ] T007 Implement `devmirror/utils/naming.py`: derive `dr_{number}_` and `qa_{number}_` schema prefixes from `dr_id`, build target schema FQNs from prod schema and configured dev catalog suffixes per `SPECIFICATION.md` 3.2.1 and 4.1
- [ ] T008 Implement `devmirror/utils/validation.py`: `dr_id` pattern, expiration vs `max_dr_duration_days`, minimum one developer, stream list non-empty
- [ ] T009 Add `devmirror/settings.py` (or equivalent) loading workspace profile, warehouse id, `control_catalog`, `control_schema` from environment variables with documented names in code docstrings
- [ ] T010 Add `tests/unit/test_config_naming_validation.py` covering happy path and failure cases for naming + validation + schema parse (fixtures only, no SDK calls)

### Implementation Notes

Follow `research.md` single admin schema for control catalog. Do not embed secrets in repo.

### Parallel Opportunities

T010 can lag T004-T008 slightly but should land in same WP for CI signal.

### Dependencies

Depends on **WP01**.

### Risks and mitigations

SDK API drift: pin version and add thin adapter in `sql_executor` so breaking changes touch one file.

---

## Work Package WP03: Control store DDL and persistence (Priority: P0)

**Goal**: Versioned DDL for UC control tables and Python repositories for DR rows, object rows, access rows, and audit append-only writes.  
**Independent Test**: Applying DDL in a sandbox schema succeeds; inserting a synthetic DR + audit round-trip via Python reads back identical fields.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP03-control-store-ddl-and-persistence.md`  
**Estimated prompt size**: ~360 lines

### Included Subtasks

- [ ] T011 Add `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/control-tables.sql` (or `devmirror/migrations/001_control_tables.sql` if you prefer code-adjacent DDL) matching `data-model.md` and `SPECIFICATION.md` 3.5
- [ ] T012 Implement `devmirror/control/control_table.py` for `devmirror_development_requests` insert/update/select by `dr_id`, status transitions used in flows 6.1-6.3
- [ ] T013 Extend same module (or sibling) for `devmirror_dr_objects` and `devmirror_dr_access` bulk insert, per-row status updates, list by `dr_id`
- [ ] T014 Implement `devmirror/control/audit.py` append and list-by-`dr_id` ordered by time
- [ ] T015 Add safe DDL apply helper (idempotent `CREATE TABLE IF NOT EXISTS` where UC allows) OR documented notebook snippet; update `quickstart.md` bootstrap section to reference the chosen path

### Implementation Notes

Use fully qualified table names `{control_catalog}.{control_schema}.table`. Align naming with `research.md` (single schema).

### Parallel Opportunities

T011 can be authored in parallel with T012-T014 once column names are frozen from `data-model.md`.

### Dependencies

Depends on **WP02** (settings + `sql_executor` for execution path).

### Risks and mitigations

UC `CREATE TABLE` permissions: document required grants for the service principal in `quickstart.md`.

---

## Work Package WP04: Scan pipeline and manifest CLI (Priority: P1) MVP core

**Goal**: Resolve streams to jobs or pipelines, query lineage, classify access modes, emit manifest YAML, expose `devmirror scan`.  
**Independent Test**: Against a workspace with known job and lineage, `devmirror scan --config ... --output manifest.yaml` produces `scan_result` matching `contracts/manifest.schema.json` (structure); unresolved stream names fail CLI with explicit list.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP04-scan-pipeline-and-manifest-cli.md`  
**Estimated prompt size**: ~420 lines

### Included Subtasks

- [ ] T016 Implement `devmirror/scan/stream_resolver.py`: resolve each `streams[].name` to job id or pipeline id using SDK (`WorkflowsAPI`, `PipelinesAPI`), fetch task graph metadata needed for downstream lineage correlation per `SPECIFICATION.md` 3.1.1
- [ ] T017 Implement `devmirror/scan/lineage.py`: read `system.access.table_lineage` (and column lineage only if required for classification) with filters by entity id where feasible
- [ ] T018 Implement `devmirror/scan/dependency_classifier.py`: map lineage edges to `READ_ONLY`, `READ_WRITE`, `WRITE_ONLY` per `SPECIFICATION.md` 3.1.1 item 4 (document heuristics in module docstring)
- [ ] T019 Implement `devmirror/scan/manifest.py`: build `scan_result` dict, set `review_required` when lineage gaps or enrichment table flags missing refs, serialize YAML
- [ ] T020 Optional hook: read curated enrichment table name from system config (`devmirror_system_config` equivalent in YAML or settings) and merge extra edges; no-op if unset
- [ ] T021 Wire `devmirror/cli.py` subcommand `scan` per `contracts/cli-commands.md`

### Implementation Notes

Start with table lineage only; column lineage is optional stretch documented in code if incomplete.

### Parallel Opportunities

T016-T019 can be split across agents only if interfaces (`StreamResolution`, `LineageGraph`) are agreed first -> prefer one agent sequential for consistency.

### Dependencies

Depends on **WP02** (SQL + settings). Does not require WP03 for writing manifest to disk; requires WP03 before persisting scan to control (later WP).

### Risks and mitigations

Incomplete lineage: always surface `review_required: true` and never auto-hide gaps.

---

## Work Package WP05: Provision core (schemas, clones, access, CLI) (Priority: P1)

**Goal**: Create prefixed schemas, execute clone strategies with revision clauses, grant dev access, orchestrate multi-object runs with partial success, persist control rows, expose `devmirror provision`.  
**Independent Test**: Dry-run mode or integration workspace: provision populates dev schemas and `devmirror_dr_objects` with `PROVISIONED` or `FAILED` per object; DR ends `ACTIVE` with warnings when partial per `SPECIFICATION.md` section 7.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP05-provision-core.md`  
**Estimated prompt size**: ~480 lines

### Included Subtasks

- [ ] T022 Implement `devmirror/provision/schema_provisioner.py` issuing `CREATE SCHEMA IF NOT EXISTS` for each required dev and optional qa target per manifest
- [ ] T023 Implement `devmirror/provision/object_cloner.py` generating SQL for `view`, `deep_clone`, `shallow_clone`, `schema_only` including `VERSION AS OF` / `TIMESTAMP AS OF` per `SPECIFICATION.md` 3.2.2-3.2.3
- [ ] T024 Implement `devmirror/provision/access_manager.py` for schema grants to principals (`SPECIFICATION.md` 3.2.3 GRANT examples) and dev catalog isolation rules
- [ ] T025 Add `devmirror/provision/runner.py` (or equivalently named orchestration module) coordinating ordered execution, `max_parallel_clones` throttling via helper from WP07 can be stubbed here with inline ThreadPoolExecutor if WP07 not merged yet -> prefer small internal pool in runner for WP05, refactor in WP07
- [ ] T026 Implement `devmirror/cli.py` subcommand `provision` for manifest path and `--auto-approve` path; update control tables via WP03 repositories; write audit entries for provision lifecycle

### Implementation Notes

Runner should record per-object errors and continue when policy allows (spec partial success). Use transactions only where UC supports multi-statement guarantees; otherwise compensate with control status.

### Parallel Opportunities

T022-T024 can be developed in parallel behind a narrow `ProvisionContext` dataclass once defined.

### Dependencies

Depends on **WP03** and **WP04**.

### Risks and mitigations

Long-running clones: log statement ids; support warehouse timeouts with retry policy documented in runner.

---

## Work Package WP06: Stream clones, safety gates, and provisioning hardening (Priority: P1)

**Goal**: Clone or rewrite workflows and pipelines for dev use, enforce review and prefix collision rules against control metadata, complete access edge cases for prod-backed views.  
**Independent Test**: Adding a second DR with colliding prefix is rejected with actionable message naming incumbent `dr_id`; `review_required` manifest cannot provision unless reviewer override flag is present in manifest metadata (define explicit field during implementation and document in contracts).  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP06-stream-clones-and-provisioning-hardening.md`  
**Estimated prompt size**: ~400 lines

### Included Subtasks

- [ ] T027 Implement `devmirror/provision/stream_cloner.py` first vertical slice: clone job with adjusted name and parameter documents for catalog mapping; pipeline path may stub with clear `NotImplementedError` until second pass if timeboxed, but interface must exist
- [ ] T028 Extend access phase so view strategy includes required `GRANT SELECT` on underlying prod objects for principals who will hit views, per `SPECIFICATION.md` provisioning flow step 7 language
- [ ] T029 Implement `--auto-approve` path: internal scan -> manifest approval metadata default for non-`review_required` only; if `review_required`, require explicit manifest field `approved_for_provision: true` (add to `contracts/manifest.schema.json` in same WP)
- [ ] T030 Implement prefix collision checks querying active DRs from control store before provisioning writes
- [ ] T031 Add structured logging context (`dr_id`, `object`, `phase`) across provision runner for operator triage

### Implementation Notes

Coordinate manifest schema change with `contracts/manifest.schema.json` and regenerate example manifests in `quickstart.md` if needed.

### Parallel Opportunities

T031 can be applied incrementally while T027-T030 land.

### Dependencies

Depends on **WP05**.

### Risks and mitigations

Job clone complexity: keep first slice minimal (name suffix, single-job clone); document unsupported pipeline features for follow-up if not v1-complete.

---

## Work Package WP07: Refresh, modify, concurrency helper (Priority: P2)

**Goal**: Refresh replicas to latest or revision; apply incremental modifications; share bounded parallelism helper used by provision and refresh.  
**Independent Test**: Active DR: `devmirror refresh` updates `last_refreshed_at` and issues `CREATE OR REPLACE` for clones per mode; `devmirror modify` adjusts user grants only without dropping unrelated objects.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP07-refresh-modify-and-concurrency.md`  
**Estimated prompt size**: ~400 lines

### Included Subtasks

- [ ] T032 Implement `devmirror/refresh/refresh_engine.py` for full, incremental, and selective scopes per `SPECIFICATION.md` 6.2
- [ ] T033 Add `devmirror/cli.py` subcommand `refresh` matching `contracts/cli-commands.md`
- [ ] T034 Implement `devmirror/modify/modification_engine.py` for add/remove objects, schemas, users, dates, add streams per `SPECIFICATION.md` 3.4 with partial success semantics
- [ ] T035 Add `devmirror/cli.py` subcommand `modify`
- [ ] T036 Ensure audit entries for refresh and modify batches with `PARTIAL_SUCCESS` when applicable
- [ ] T037 Extract reusable bounded concurrency helper to `devmirror/utils/concurrent.py` and refactor WP05 runner to use it for `max_parallel_clones`

### Implementation Notes

If WP05 inlined ThreadPoolExecutor, replace with helper here without behavior regression.

### Parallel Opportunities

T032-T035 sequential recommended; T037 refactor can follow immediately after T032 exists.

### Dependencies

Depends on **WP05**.

### Risks and mitigations

Delta retention validation: centralize revision validation helper shared by refresh and provision.

---

## Work Package WP08: Lifecycle cleanup, notifications, remaining CLI, and ops docs (Priority: P2-P4)

**Goal**: Daily notify and cleanup loops, CLI `cleanup`, `status`, `list`, scheduler entrypoints, Databricks job assets, root README.  
**Independent Test**: Simulated DR past expiration transitions to `CLEANUP_IN_PROGRESS` then `CLEANED_UP` or retryable state; notification sends once (`notification_sent_at` set); CLI `status` shows counts and timestamps.  
**Prompt**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/tasks/WP08-lifecycle-cli-and-ops-docs.md`  
**Estimated prompt size**: ~420 lines

### Included Subtasks

- [ ] T038 Implement `devmirror/cleanup/cleanup_engine.py` ordered drops, revokes, cloned job removal per `SPECIFICATION.md` 3.6.3
- [ ] T039 Implement `devmirror/cleanup/notifier.py` for pre-expiry selection query and notification dispatch abstraction (Databricks destination first, SMTP stub acceptable behind interface)
- [ ] T040 Add CLI subcommands `cleanup`, `status`, `list` per `contracts/cli-commands.md`
- [ ] T041 Add `devmirror/jobs.py` (or `devmirror/scheduled.py`) entrypoints `run_notifications` and `run_cleanup` callable from Databricks jobs
- [ ] T042 Add `databricks/` asset bundle or minimal `databricks.yml` plus two job definitions referencing the Python entrypoints and cron defaults from `SPECIFICATION.md` section 8
- [ ] T043 Update repository root `README.md` linking `SPECIFICATION.md`, feature spec, quickstart, environment variables, and operator runbook for scheduled jobs

### Implementation Notes

Keep secrets out of git; document required job parameters and job cluster or serverless SQL warehouse policy.

### Parallel Opportunities

T042 and T043 can proceed in parallel after T041 function signatures are stable.

### Dependencies

Depends on **WP03**, **WP05**, and **WP07** (concurrency helper optional for cleanup; WP07 ensures grant revoke parity with modify paths).

### Risks and mitigations

Email double-send: guard with `notification_sent_at` exactly as spec.

---

## Dependency and execution summary

- **Sequence**: WP01 -> WP02 -> WP03 -> WP04 -> WP05 -> WP06 -> WP07 -> WP08  
- **Parallelization after WP02**: WP03 and WP04 could overlap in staffing only if WP04 avoids control writes until WP03 lands -> recommended linear order above for solo implementer.  
- **MVP scope**: WP01 through WP05 plus minimal WP06 (T027 stub acceptable) delivers first provisioned DR; full v1 requires WP06-WP08.

---

## Subtask index (reference)

| Subtask | Summary | Work package |
|---------|---------|---------------|
| T001-T005 | Packaging and config | WP01 |
| T006-T010 | SQL wrapper, naming, validation, settings, unit tests | WP02 |
| T011-T015 | DDL and control repositories | WP03 |
| T016-T021 | Scan and manifest CLI | WP04 |
| T022-T026 | Provision core | WP05 |
| T027-T031 | Stream clone and hardening | WP06 |
| T032-T037 | Refresh, modify, concurrency | WP07 |
| T038-T043 | Cleanup, notify, CLI, jobs, README | WP08 |
