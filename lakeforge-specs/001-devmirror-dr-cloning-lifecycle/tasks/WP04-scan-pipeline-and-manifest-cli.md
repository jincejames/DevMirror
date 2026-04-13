---
work_package_id: WP04
title: Scan pipeline and manifest CLI
lane: planned
dependencies: []
subtasks:
- T016
- T017
- T018
- T019
- T020
- T021
phase: Phase 2 - Discovery
assignee: ''
agent: ''
shell_pid: ''
review_status: ''
reviewed_by: ''
history:
- timestamp: '2026-04-13T20:00:00Z'
  lane: planned
  agent: system
  shell_pid: ''
  action: Prompt generated via /tasks
---

# Work Package Prompt: WP04 - Scan pipeline and manifest CLI

## Review Feedback

*[Empty at creation.]*

---

## Objectives and success criteria

- Each configured stream resolves to a concrete job or pipeline definition in production scope.
- Lineage query builds dependency set sufficient for manifest generation.
- Manifest YAML validates against `contracts/manifest.schema.json` structure and sets `review_required` when lineage is incomplete.
- CLI `devmirror scan` writes output file and exits non-zero on unresolved streams with explicit list.

## Context and constraints

- **SPECIFICATION**: Section 3.1 (scan module), 6.1 steps 1-3.
- **Contracts**: `manifest.schema.json`, `cli-commands.md`.
- **Spec user story**: P1 story 1 acceptance scenario 1 and 3.
- **Dependencies**: WP02 for settings and `SqlExecutor`. WP03 not required for file-only manifest output.

## Implementation command

```bash
lakeforge implement WP04 --base WP02
```

Note: `finalize-tasks` may still linearize; if your tooling expects strict WP03 before WP04, follow `tasks.md` dependency graph: WP04 lists WP02 only.

---

## Subtasks and detailed guidance

### Subtask T016 - stream_resolver

- **Purpose**: Map human stream names to Databricks resources.
- **Steps**:
  1. Define resolution strategy: exact job name in workspace vs fully qualified including project folder if your org uses Jobs API 2.1 `name` searches -> document chosen strategy in module docstring.
  2. Implement `resolve_job_by_name(client, name) -> Job` using `w.jobs.list` filters or `name` query parameter supported by SDK version pinned in WP01.
  3. Implement `resolve_pipeline_by_name(client, name) -> PipelineState` or pipeline settings object similarly.
  4. If ambiguous matches, fail with list of candidates (spec: clear errors).
  5. Return structure capturing `workflow_id` or `pipeline_id` and task keys list when available for lineage correlation.
- **Files**: `devmirror/scan/stream_resolver.py`.
- **Parallel?**: After interfaces defined.
- **Validation**: Mock SDK list responses in unit tests.

### Subtask T017 - lineage queries

- **Purpose**: Pull read and write edges from system tables.
- **Steps**:
  1. Query `system.access.table_lineage` filtered by source or target entity identifiers tied to resolved job or pipeline where UC exposes such linkage; if UC only supports table-centric queries, document iterative approach: extract notebook SQL paths is out of scope for v1 unless already in SPEC -> stick to lineage tables per SPEC 3.1.1 item 2.
  2. Normalize output to internal graph type: nodes are UC FQNs, edges have operation type if available.
  3. Cap row volume with configurable limit and set `review_required` if limit hit.
- **Files**: `devmirror/scan/lineage.py`.
- **Parallel?**: After T016 returns ids.
- **Validation**: Integration-only test acceptable behind flag; unit test with canned rows.

### Subtask T018 - dependency_classifier

- **Purpose**: Label each object with `READ_ONLY`, `READ_WRITE`, `WRITE_ONLY` per SPEC 3.1.1 item 4.
- **Steps**:
  1. Implement heuristics: if only read operations in lineage for object vs pipeline writes -> classify. If ambiguous, default to `READ_ONLY` with `review_required` true OR conservative `READ_WRITE` -> document choice.
  2. Input: lineage graph + optional stream output targets from job definition if parseable.
- **Files**: `devmirror/scan/dependency_classifier.py`.
- **Parallel?**: After T017.
- **Validation**: Golden graph fixtures in `tests/unit/test_classifier.py`.

### Subtask T019 - manifest builder

- **Purpose**: Serialize `scan_result` YAML for human review.
- **Steps**:
  1. Build Python dict matching `scan_result` nested structure from contracts.
  2. Include `schemas_required` unique sorted list.
  3. Serialize with `yaml.safe_dump` preserving order where possible.
  4. Validate output with `jsonschema` optional dependency OR skip runtime schema validation but add dev test using `jsonschema` if acceptable dependency -> if not, structural assertions only.
- **Files**: `devmirror/scan/manifest.py`.
- **Parallel?**: After T018.
- **Validation**: Output parses back and matches keys in `manifest.schema.json`.

### Subtask T020 - optional enrichment hook

- **Purpose**: Merge curated dependencies not in automatic lineage.
- **Steps**:
  1. Read optional table name from settings, for example `DEVMIRROR_LINEAGE_ENRICHMENT_TABLE` as `catalog.schema.table`.
  2. If set, `SELECT stream_key, object_fqn, access_hint FROM ... WHERE stream_key IN (...)` (define minimal contract in code comments; actual columns are implementer choice but must be documented).
  3. Merge into graph before classifier; mark `review_required` if enrichment missing expected rows.
- **Files**: `devmirror/scan/manifest.py` or `devmirror/scan/enrichment.py`.
- **Parallel?**: After T017.
- **Validation**: Unit test with enrichment disabled (no-op path).

### Subtask T021 - CLI scan

- **Purpose**: Operator entrypoint.
- **Steps**:
  1. Use `click` group `devmirror` with subcommand `scan` accepting `--config` and `--output`.
  2. Load config via WP01 loader, build workspace client, call resolver then lineage then classifier then manifest writer.
  3. Exit code 2 for validation errors, 1 for resolution failures, 0 on success.
- **Files**: `devmirror/cli.py`.
- **Parallel?**: After scan pipeline functions exist.
- **Validation**: Smoke test with mocked client layers.

## Test strategy

Unit tests for resolver (mock), classifier (fixtures), manifest structure. Integration optional.

## Risks and mitigations

- **Risk**: Lineage blind spots. **Mitigation**: Always surface `review_required` and never silent success.

## Review guidance

- Re-read SPEC 3.1.3 human review bullets and ensure manifest carries enough fields for reviewer edits (even if WP06 adds approval flags later).

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
