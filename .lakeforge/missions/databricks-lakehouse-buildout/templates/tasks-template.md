---
description: "Work package task list template for lakehouse feature implementation"
---

# Work Packages: [FEATURE NAME]

**Inputs**: Design documents from `/lakeforge-specs/[###-feature-name]/`
**Prerequisites**: plan.md (required), spec.md, data-model.md

**Organization**: Work packages organized by lakehouse phases:
- Phase 1: Bronze Layer (raw data landing)
- Phase 2: Silver Layer (validation, transformation)
- Phase 3: Gold Layer (customer-facing delivery)
- Phase 4: Documentation and Close

## Path Conventions

- **Bronze tables**: `dw_bronze.`
- **Silver tables**: `dw_silver.`
- **Gold tables**: `gold_etl.`
- **Gold views**: `gold.`
- **Gold history**: `gold_hist.`

## Subtask Format: `[Txxx] [P?] Description`

- **[P]** indicates the subtask can proceed in parallel (different sources/entities).
- Include precise table names and database paths.

---

## Phase 1: Bronze Layer

### Work Package WP01: Bronze Infrastructure (Priority: P0)

**Goal**: Establish Bronze schemas and base tables for raw data landing.
**Independent Test**: Bronze tables created and accepting data.
**Prompt**: `/tasks/WP01-bronze-infrastructure.md`

#### Included Subtasks
- [ ] T001 Create Bronze schema `dw_bronze`
- [ ] T002 [P] Create source connector configuration
- [ ] T003 [P] Create landing tables with metadata columns

#### Implementation Notes
- Add `_ingested_at` and `_schema_version` columns to all tables
- Use proper connectors (not generic JDBC)
- No transformations - raw data only

#### Parallel Opportunities
- Multiple source connectors can be developed in parallel

#### Dependencies
- None (starting package)

---

### Work Package WP02: Bronze Landing (Priority: P0)

**Goal**: Implement data landing for each source.
**Independent Test**: Data lands in Bronze tables with metadata.
**Prompt**: `/tasks/WP02-bronze-landing.md`

#### Included Subtasks
- [ ] T004 [P] Implement landing for Source A
- [ ] T005 [P] Implement landing for Source B
- [ ] T006 Landing validation (distinct counts, time ranges)

#### Implementation Notes
- Use Auto Loader for incremental file ingestion
- Use `COPY INTO` for batch file ingestion
- Validate each landing before proceeding

#### Parallel Opportunities
- Source landings can proceed in parallel

#### Dependencies
- Depends on WP01

---

## Phase 2: Silver Layer

### Work Package WP03: Silver Transformation (Priority: P1)

**Goal**: Transform and historize data from Bronze to Silver.
**Independent Test**: Silver tables populated with CDC and historization.
**Prompt**: `/tasks/WP03-silver-transformation.md`

#### Included Subtasks
- [ ] T007 Create Silver schema `dw_silver`
- [ ] T008 [P] Implement mappings from Bronze
- [ ] T009 Implement CDC (Type-2 SCD)
- [ ] T010 Add validation rules

#### Implementation Notes
- All transformations complete in Silver (not Gold)
- Type-2 SCD with `start_date`, `end_date`, `is_current`
- Validate before moving to Gold

#### Dependencies
- Depends on WP02 (Bronze data must be landing)

---

### Work Package WP04: Silver Validation (Priority: P1)

**Goal**: Implement pre-Gold validation rules.
**Independent Test**: Validation rules pass for clean data, fail for bad data.
**Prompt**: `/tasks/WP04-silver-validation.md`

#### Included Subtasks
- [ ] T011 Implement range validation
- [ ] T012 Implement cross-field validation
- [ ] T013 Create quarantine table for failed records
- [ ] T014 Implement alerting for validation failures

#### Implementation Notes
- Quality gates are NON-NEGOTIABLE
- Failed records must be routed (not silently dropped)

#### Dependencies
- Depends on WP03 (Silver data must exist)

---

## Phase 3: Gold Layer

### Work Package WP05: Gold Delivery (Priority: P2)

**Goal**: Create customer-facing views and optimize tables.
**Independent Test**: Gold views accessible and performant.
**Prompt**: `/tasks/WP05-gold-delivery.md`

#### Included Subtasks
- [ ] T015 Create Gold schemas (`gold_etl`, `gold`, `gold_hist`)
- [ ] T016 Create Gold tables
- [ ] T017 Create customer-facing views
- [ ] T018 Implement quality gates

#### Implementation Notes
- Views only for customer access (no direct tables)
- Explicit column naming (no SELECT *)
- Quality gates must pass before data moves to Gold

#### Dependencies
- Depends on WP04 (Silver validation must be ready)

---

### Work Package WP06: Gold Optimization (Priority: P2)

**Goal**: Optimize Gold tables for query performance.
**Independent Test**: Query performance meets SLA.
**Prompt**: `/tasks/WP06-gold-optimization.md`

#### Included Subtasks
- [ ] T019 Apply Z-ordering on filter columns
- [ ] T020 Compute statistics for key columns
- [ ] T021 Configure ACLs
- [ ] T022 Create history tables (`_hist` suffix)

#### Implementation Notes
- Auto-optimize OFF for Z-ordered tables
- Compute statistics: `ANALYZE TABLE`
- Proper ACL configuration for security

#### Dependencies
- Depends on WP05 (Gold tables must exist)

---

## Phase 4: Documentation and Close

### Work Package WP07: Documentation (Priority: P3)

**Goal**: Document the implementation and hand off.
**Independent Test**: Documentation complete and accessible.
**Prompt**: `/tasks/WP07-documentation.md`

#### Included Subtasks
- [ ] T023 Technical documentation
- [ ] T024 Runbook for operations
- [ ] T025 Data dictionary

#### Implementation Notes
- Never skip documentation (minimum 1 day)
- Document along the way

#### Dependencies
- Depends on WP05, WP06

---

## Dependency & Execution Summary

```
Phase 1 (Bronze)
  └─→ Phase 2 (Silver)
       └─→ Phase 3 (Gold)
            └─→ Phase 4 (Documentation)

Parallelization within Phase 1:
  Source A ──┐
  Source B ──┼─→ Phase 2 (when all sources ready)
  Source C ──┘
```

- **MVP Scope**: Phase 1 + Phase 2 + Phase 3 (functional pipeline)
- **Full Scope**: All 4 phases including documentation

---

## Subtask Index (Reference)

| Subtask ID | Summary | Work Package | Phase | Parallel? |
|------------|---------|--------------|-------|-----------|
| T001 | Create Bronze schema | WP01 | Bronze | No |
| T002 | Source connector config | WP01 | Bronze | Yes |
| T003 | Landing tables | WP01 | Bronze | Yes |
| T004-T005 | Source landing | WP02 | Bronze | Yes |
| T006 | Landing validation | WP02 | Bronze | No |
| T007-T010 | Silver transformation | WP03 | Silver | Partial |
| T011-T014 | Silver validation | WP04 | Silver | Partial |
| T015-T018 | Gold delivery | WP05 | Gold | No |
| T019-T022 | Gold optimization | WP06 | Gold | Partial |
| T023-T025 | Documentation | WP07 | Docs | Yes |

---

> Replace all placeholder text above with feature-specific content. Keep this template structure intact so downstream automation can parse work packages reliably.
