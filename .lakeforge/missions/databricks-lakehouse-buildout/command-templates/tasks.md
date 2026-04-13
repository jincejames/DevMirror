---
description: Generate work packages organized by lakehouse phases.
---

# /tasks - Generate Lakehouse Work Packages

**Version**: 0.11.0+

## ⚠️ CRITICAL: THIS IS THE MOST IMPORTANT PLANNING WORK

**You are creating the blueprint for lakehouse implementation**. The quality of work packages determines:
- How easily agents can implement each layer
- How parallelizable the work is
- How reviewable the code will be
- Whether the lakehouse succeeds or fails

**QUALITY OVER SPEED**: This is NOT the time to save tokens or rush.

---

## 📍 WORKING DIRECTORY: Stay in MAIN repository

**IMPORTANT**: Tasks works in the main repository. NO worktrees created.

```bash
# Run from project root (same directory as /plan):
# Creates:
# - lakeforge-specs/###-feature/tasks/WP01-*.md → In main repository
# - lakeforge-specs/###-feature/tasks/WP02-*.md → In main repository
# - Commits ALL to main branch
# - NO worktrees created
```

**Worktrees created later**: After tasks are generated, use `lakeforge implement WP##` to create workspace for each WP.

## Databricks Documentation Reference

**CRITICAL**: gather current Databricks context based on the information you already have:

### Step 1: Fetch the sitemap
Use `WebFetch` (not WebSearch) to get the documentation index:
WebFetch:
  url: "https://docs.databricks.com/llms.txt"
  prompt: "List all URLs related to best practices, data engineering, ETL, pipelines, and architecture"

### Step 2: Fetch relevant best practices pages
Based on the sitemap, fetch specific documentation:
WebFetch:
  url: "https://docs.databricks.com/getting-started/best-practices"
  prompt: "Extract all best practices and recommendations"

WebFetch:
  url: "https://docs.databricks.com/dev-tools/ci-cd/best-practices"
  prompt: "Extract CI/CD guidelines and workflows"

### Step 3: Fetch topic-specific docs based on user context
If user mentions streaming → fetch streaming docs
If user mentions ML → fetch MLOps docs
If user mentions ETL → fetch pipeline docs

**Purpose**:
- Ensure questions use current Databricks terminology
- Include documentation links when referencing features
- Auto-correct outdated terms with explanation

**Example auto-correction**:
> User: "I want to use DLT"
> Agent: "Note: DLT is now called 'Lakeflow Declarative Pipelines'. [See docs](https://docs.databricks.com/ldp/)"

If WebSearch fails, proceed with built-in knowledge and note: "Databricks documentation unavailable - using built-in patterns."

## Best Practices Enforcement (STRICT)

**CRITICAL**: Work packages MUST only contain tasks that align with Databricks best practices.

**Task generation rules**:
1. **Never create tasks for non-compliant patterns** - If plan contains something contradicting best practices, flag it and create the best-practice alternative task instead
2. **Bronze-first is non-negotiable** - Every data source gets Bronze landing tasks before Silver
3. **Quality gates required** - Every feature gets quality gate tasks between Silver and Gold
4. **Proper tooling** - Tasks must specify proper connectors, Delta Lake, Unity Catalog

**If plan requests non-compliant tasks**:
> "The plan requests [non-compliant pattern]. This contradicts Databricks best practices. Creating tasks for [best-practice alternative] instead. [doc link]"

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Location Check (0.11.0+)

Before proceeding, verify you are in the main repository:

```bash
git branch --show-current
```

**Expected output:** `main` (or `master`)

Work packages are generated directly in `lakeforge-specs/###-feature/` and committed to main.

## Lakehouse-Phased Work Package Organization

Work packages MUST be organized by lakehouse phases:

### Phase 1: Bronze Layer (Foundation)

**Goal**: Land raw data with zero transformations

Typical WPs:
- WP01: Bronze infrastructure (schemas, tables)
- WP02: Source connector implementation
- WP03: Landing validation

**Parallelization**: Multiple sources can be landed in parallel

**Subtask examples**:
- T001 Create Bronze schema `dw_bronze`
- T002 [P] Create source connector configuration for Source A
- T003 [P] Create source connector configuration for Source B
- T004 [P] Create landing tables with metadata columns
- T005 Implement landing validation (distinct counts, time ranges)

### Phase 2: Silver Layer (Transformation)

**Goal**: Validate, transform, and historize data

**Dependencies**: Depends on Phase 1 completion for relevant sources

Typical WPs:
- WP04: Mapping implementation
- WP05: CDC/SCD Type-2 implementation
- WP06: Silver validation

**Subtask examples**:
- T006 Create Silver schema `dw_silver`
- T007 Implement mappings from Bronze
- T008 Implement CDC (Type-2 SCD with start_date, end_date, is_current)
- T009 Add validation rules before Gold promotion

### Phase 3: Gold Layer (Delivery)

**Goal**: Create customer-facing views and optimize

**Dependencies**: Depends on Phase 2 completion

Typical WPs:
- WP07: Gold view creation
- WP08: Quality gate implementation
- WP09: Optimization (Z-ordering, statistics)
- WP10: Security configuration (ACLs)

**Subtask examples**:
- T010 Create Gold schemas (`gold_etl`, `gold`, `gold_hist`)
- T011 Create Gold tables with explicit column naming
- T012 Create customer-facing views (no SELECT *)
- T013 Implement quality gates (range, cross-field, statistical)
- T014 Apply optimization (Z-ordering, statistics)
- T015 Configure ACLs

### Phase 4: Documentation (Close)

**Goal**: Document and hand off

**Dependencies**: Depends on Phases 1-3 completion

Typical WPs:
- WP11: Technical documentation
- WP12: Runbook and operations guide

**Subtask examples**:
- T016 Technical documentation (architecture, data flows)
- T017 Runbook for operations
- T018 Data dictionary (column definitions, business rules)

### Phase Dependencies Diagram

```
Phase 1 (Bronze)
  └─→ Phase 2 (Silver)
       └─→ Phase 3 (Gold)
            └─→ Phase 4 (Docs)

Parallelization within Phase 1:
  Source A Bronze ──┐
  Source B Bronze ──┼─→ Phase 2 (when all sources ready)
  Source C Bronze ──┘
```

## Outline

### Step 1: Setup

Run `lakeforge agent feature check-prerequisites --json --paths-only --include-tasks` and capture `FEATURE_DIR`.

### Step 2: Load Design Documents

Read from `FEATURE_DIR`:
- spec.md (required) - Data sources, layer requirements
- plan.md (required) - Lakehouse architecture design
- data-model.md (optional) - Entity definitions

### Step 3: Derive ALL Subtasks

Create complete list of subtasks with IDs T001, T002, etc.

**Lakehouse-specific subtask categories**:
- Bronze landing (per source)
- Bronze validation
- Silver transformation (per entity)
- Silver CDC/historization
- Silver validation
- Gold table creation
- Gold view creation
- Quality gate implementation
- Optimization (Z-ordering, statistics)
- Security (ACLs)
- Documentation

### Step 4: Group into Work Packages by Phase

**Grouping by lakehouse phase**:

| Phase | WP Range | Goal |
|-------|----------|------|
| Bronze | WP01-WP03 | Land raw data |
| Silver | WP04-WP06 | Transform and historize |
| Gold | WP07-WP10 | Deliver to customers |
| Docs | WP11-WP12 | Document and close |

**Sizing guidelines**:
- Target: 3-7 subtasks per WP (200-500 line prompts)
- Maximum: 10 subtasks per WP (700 line prompts)
- If more than 10 subtasks: Create additional WPs

### Step 5: Write tasks.md

Use the lakehouse tasks template (`src/specify_cli/missions/databricks-lakehouse-buildout/templates/tasks-template.md`):

- **Location**: Write to `FEATURE_DIR/tasks.md`
- Organize by lakehouse phases (Bronze, Silver, Gold, Docs)
- Include path conventions (dw_bronze., dw_silver., gold_etl., gold., gold_hist.)
- Include phase dependency diagram

### Step 6: Generate WP Prompt Files

For each WP, generate `FEATURE_DIR/tasks/WPxx-slug.md`:

**Include Databricks Documentation Reference section**:
```markdown
## Databricks Documentation Reference

**Before implementing**, fetch relevant documentation:
- **Sitemap**: https://docs.databricks.com/llms.txt

**Topics for this WP**:
[Layer-specific topics]
```

**Include correct implementation command**:
- Phase 1 WPs: `lakeforge implement WP01`
- Phase 2 WPs: `lakeforge implement WP04 --base WP03`
- Phase 3 WPs: `lakeforge implement WP07 --base WP06`
- Phase 4 WPs: `lakeforge implement WP11 --base WP10`

### Step 7: Finalize Tasks

Run `lakeforge agent feature finalize-tasks --json` to:
- Parse dependencies
- Update frontmatter
- Validate (cycles, invalid refs)
- Commit to main

### Step 8: Report

Provide summary with:
- WP count and subtask tallies per phase
- Phase dependencies
- Parallelization opportunities (especially within Bronze)
- MVP scope (typically Phase 1 + Phase 2 + Phase 3)

## Dependency Detection (0.11.0+)

**Generate dependencies in WP frontmatter based on lakehouse phases**:

```yaml
---
work_package_id: "WP04"
title: "Silver Transformation"
phase: "Phase 2 - Silver Layer"
lane: "planned"
dependencies: ["WP01", "WP02", "WP03"]  # All Bronze WPs
subtasks: ["T006", "T007", "T008", "T009"]
---
```

**Phase-based dependency patterns**:
- Phase 1 WPs: No dependencies (foundation)
- Phase 2 WPs: Depend on relevant Phase 1 WPs
- Phase 3 WPs: Depend on relevant Phase 2 WPs
- Phase 4 WPs: Depend on all previous phases

## Work Package Sizing Guidelines

### Ideal WP Size

**Target: 3-7 subtasks per WP**
- Results in 200-500 line prompt files
- Agent can hold entire context
- Clear scope - easy to review

**Examples of well-sized lakehouse WPs**:

- WP01: Bronze Infrastructure (4 subtasks, ~280 lines)
  - T001: Create Bronze schema
  - T002: Configure connector for Source A
  - T003: Create landing tables
  - T004: Add metadata columns

- WP04: Silver Transformation (5 subtasks, ~350 lines)
  - T010: Create Silver schema
  - T011: Implement mappings
  - T012: Implement CDC (Type-2 SCD)
  - T013: Add historization columns
  - T014: Pre-Gold validation

### Maximum WP Size

**Hard limit: 10 subtasks, ~700 lines**

If you need more than 10 subtasks: SPLIT by sub-concern within the phase.

Example split for complex Bronze:
- WP01: Bronze Schema & Core Tables (5 subtasks)
- WP02: Bronze Source Connectors (5 subtasks)
- WP03: Bronze Validation & Optimization (4 subtasks)

## Remember

**Lakehouse architecture requires disciplined layer progression.**

A well-crafted set of phase-organized work packages ensures:
- Bronze lands cleanly before Silver transforms
- Silver validates before Gold publishes
- Quality gates prevent bad data from reaching customers
- Documentation captures the full data lineage

**Invest the tokens now. Be thorough. Future data engineers will thank you.**
