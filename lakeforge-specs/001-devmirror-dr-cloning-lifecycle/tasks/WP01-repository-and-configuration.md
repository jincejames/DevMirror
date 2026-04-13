---
work_package_id: "WP01"
title: "Repository and configuration"
phase: "Phase 1 - Foundation"
lane: "planned"
dependencies: []
subtasks:
  - "T001"
  - "T002"
  - "T003"
  - "T004"
  - "T005"
assignee: ""
agent: ""
shell_pid: ""
review_status: ""
reviewed_by: ""
history:
  - timestamp: "2026-04-13T20:00:00Z"
    lane: "planned"
    agent: "system"
    shell_pid: ""
    action: "Prompt generated via /tasks"
---

# Work Package Prompt: WP01 - Repository and configuration

## Review Feedback

*[Empty at creation. Reviewers populate if work returns from review.]*

---

## Markdown Formatting

Wrap HTML or XML fragments in backticks. Use fenced code blocks with language tags.

---

## Objectives and success criteria

- Repository installs as an editable Python package with a `devmirror` console entrypoint (stub implementation acceptable until later WPs).
- Pydantic models validate the `development_request` YAML shape in alignment with `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/config.schema.json` and `SPECIFICATION.md` section 2.1.
- Loaders read YAML from disk and raise actionable errors on parse or validation failure.

## Context and constraints

- **Spec**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/spec.md` (FR-001, configuration validation table).
- **Plan**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/plan.md` (Technical Context, source tree).
- **Contracts**: `lakeforge-specs/001-devmirror-dr-cloning-lifecycle/contracts/config.schema.json`.
- **Encoding**: Follow `.lakeforge/AGENTS.md` (ASCII-friendly punctuation in docs you add).
- **No secrets** in repository files.

## Implementation command

```bash
lakeforge implement WP01
```

No `--base` flag (first package in the chain).

---

## Subtasks and detailed guidance

### Subtask T001 - Add pyproject.toml with package metadata

- **Purpose**: Establish installable distribution and dependency pins before any feature code imports SDKs.
- **Steps**:
  1. Create `pyproject.toml` using PEP 621 metadata: name `devmirror` (or `databricks-devmirror` if you need PyPI uniqueness later; for monorepo local package `devmirror` matches `plan.md`).
  2. Require Python `>=3.11,<3.14` to match plan and local metadata.
  3. Declare runtime deps: `pydantic>=2`, `pyyaml`, `databricks-sdk`, `click`.
  4. Declare optional dev: `pytest`, `ruff`.
  5. Define `[project.scripts]` mapping `devmirror = "devmirror.cli:main"` (adjust import path once `cli.py` exists).
  6. Set `packages = [{ include = "devmirror" }]` under build backend `hatchling` or `setuptools` (pick one standard backend).
- **Files**: `pyproject.toml` (new).
- **Parallel?**: No (foundation).
- **Validation**: `python -m pip install -e .` from repo root succeeds; `devmirror --help` prints help or placeholder.

### Subtask T002 - Create devmirror package skeleton

- **Purpose**: Mirror `SPECIFICATION.md` section 5.2 so later WPs can land modules without merge conflicts on structure.
- **Steps**:
  1. Create `devmirror/__init__.py` with `__version__`.
  2. Add empty `devmirror/cli.py` with `def main(): ...` raising `NotImplementedError` or printing stub help until WP04-WP08 flesh out subcommands.
  3. Create package dirs: `config`, `scan`, `provision`, `refresh`, `modify`, `cleanup`, `control`, `utils` each with `__init__.py`.
- **Files**: Under `devmirror/` as listed.
- **Parallel?**: After T001 partial (can start once pyproject exists).
- **Validation**: `python -c "import devmirror"` succeeds.

### Subtask T003 - Configure ruff and pytest

- **Purpose**: Enforce consistent style and enable fast tests in WP02.
- **Steps**:
  1. Add `ruff` section to `pyproject.toml` (line length 100 or 120, select reasonable rule set).
  2. Add `[tool.pytest.ini_options]` with `testpaths = ["tests"]`.
  3. Create `tests/__init__.py` (empty) and `tests/unit/.gitkeep` if needed.
- **Files**: `pyproject.toml`, `tests/` tree.
- **Parallel?**: Yes with T004 if coordinated on `pyproject.toml` merges.
- **Validation**: `ruff check devmirror` passes; `pytest` runs zero tests successfully.

### Subtask T004 - Implement config schema (Pydantic)

- **Purpose**: Single source of truth for YAML validation shared by CLI and future notebooks.
- **Steps**:
  1. Implement nested models for `development_request` mirroring JSON Schema: `streams`, `additional_objects`, `environments` (dev required enabled true, qa optional), `data_revision` with mode-specific required fields (`version` when mode is `version`, `timestamp` when mode is `timestamp`).
  2. Use Pydantic v2 field validators for cross-field rules (for example `expiration_date` parse to `date` type).
  3. Export a top-level function `parse_config(path: Path) -> DevelopmentRequestConfig` or similar from `devmirror.config`.
  4. Align field names with `SPECIFICATION.md` YAML keys exactly to avoid user confusion.
- **Files**: `devmirror/config/schema.py`, `devmirror/config/__init__.py` re-exports.
- **Parallel?**: After T002.
- **Validation**: Unit tests added in WP02 will exercise; for WP01 add minimal inline test in module `if __name__ == "__main__"` only if you must, prefer waiting for WP02.

### Subtask T005 - Implement config loader

- **Purpose**: Read file from operator path and bridge YAML parsing to Pydantic models.
- **Steps**:
  1. Implement `load_development_request(path: Path) -> DevelopmentRequestConfig` using `yaml.safe_load` then model_validate.
  2. Catch `yaml.YAMLError` and `pydantic.ValidationError`; re-raise or wrap in `DevMirrorConfigError` with message including file path and humanized validation errors.
  3. Do not load arbitrary tags; keep safe_load only.
- **Files**: `devmirror/config/loader.py`.
- **Parallel?**: Depends on T004 models existing.
- **Validation**: Invalid fixture file raises `DevMirrorConfigError` with clear text; valid minimal YAML from spec example loads.

## Test strategy

Defer exhaustive tests to WP02 except optional smoke in T001 validation command.

## Risks and mitigations

- **Risk**: JSON Schema and Pydantic drift. **Mitigation**: Add a one-way test in WP02 that loads `contracts/config.schema.json` only if you add jsonschema dependency; otherwise document manual sync in `contracts/README.md`.

## Review guidance

- Confirm `pyproject.toml` pins are not overly loose on `databricks-sdk`.
- Confirm no credentials or workspace hostnames in repo.
- Confirm `devmirror/cli.py` entrypoint exists even if stub.

## Activity Log

- 2026-04-13T20:00:00Z - system - lane=planned - Prompt created
