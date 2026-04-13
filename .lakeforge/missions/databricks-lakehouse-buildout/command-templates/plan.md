---
description: Design layer-by-layer implementation plan for lakehouse features.
---

# /plan - Create Lakehouse Implementation Plan

**Version**: 0.11.0+

## 📍 WORKING DIRECTORY: Stay in MAIN repository

**IMPORTANT**: Plan works in the main repository. NO worktrees created.

```bash
# Run from project root (same directory as /specify):
# You should already be here if you just ran /specify

# Creates:
# - lakeforge-specs/###-feature/plan.md → In main repository
# - Commits to main branch
# - NO worktrees created
```

**Do NOT cd anywhere**. Stay in the main repository root.

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

**CRITICAL**: This mission enforces strict adherence to Databricks best practices in ALL planning decisions.

**Planning must align with documented best practices**:
1. **Architecture decisions**: Only propose patterns documented as best practices
2. **Technology choices**: Delta Lake, Unity Catalog, proper connectors (not alternatives)
3. **Layer design**: Bronze-first landing, no layer skipping, quality gates before Gold

**REFUSE to plan non-compliant approaches**:
- If spec describes direct Silver/Gold landing → Plan Bronze landing first, explain why
- If spec mentions DBFS for production → Plan Unity Catalog managed tables instead
- If spec requests SELECT * in views → Plan explicit column naming

**Example enforcement in planning**:
> Spec: "Land customer data directly in Silver for faster development"
> Agent: "I cannot plan direct Silver landing. Databricks best practices require Bronze-first architecture. [docs.databricks.com/...] The plan will include Bronze landing with streamlined validation to maintain speed while following architectural standards."

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Location Check (0.11.0+)

This command runs in the **main repository**, not in a worktree.

- Verify you're on `main` (or `master`) before scaffolding plan.md
- Planning artifacts live in `lakeforge-specs/###-feature/`
- The plan template is committed to the main branch after generation

**Path reference rule:** When you mention directories or files, provide either the absolute path or a path relative to the project root.

## Planning Interrogation (mandatory)

Before executing any scripts or generating artifacts you must interrogate the specification and stakeholders.

### Lakehouse Planning Questions

In addition to standard planning questions, ask:

**Bronze Layer Design**:
- What connectors will be used for each source? (not generic JDBC)
- What metadata columns beyond standard (_ingested_at, _schema_version)?
- What is the landing validation strategy?

**Silver Layer Design**:
- What transformation rules apply?
- What is the CDC/historization strategy?
- What partitioning scheme?

**Gold Layer Design**:
- What views will be created?
- What optimization strategy (Z-ordering columns)?
- What ACL configuration?

**Quality Gates**:
- What validation rules must pass before Gold promotion?
- How are failed records handled?

### Scope Proportionality

- **Scope proportionality (CRITICAL)**: FIRST, assess the feature's complexity from the spec:
  - **Trivial/Test Features** (single source, simple pipeline): Ask 1-2 questions about connectors, then proceed
  - **Simple Features** (few sources, standard transformations): Ask 2-3 questions about transformation rules
  - **Complex Features** (multi-source, complex CDC): Ask 3-5 questions covering architecture, performance requirements
  - **Platform/Critical Features** (core data infrastructure): Full interrogation with 5+ questions

- **User signals to reduce questioning**: If the user says "use defaults", "standard lakehouse", "skip to implementation" - use standard patterns.

### Planning Requirements

1. Maintain a **Planning Questions** table internally. Do **not** render this table to the user.
2. For simple features, standard lakehouse practices are acceptable.
3. When you have sufficient context, summarize into an **Engineering Alignment** note and confirm.

## Outline

### 1. Check Planning Discovery Status

- If any planning questions remain unanswered or the user has not confirmed the **Engineering Alignment** summary, stay in the one-question cadence.
- Once confirmed, continue.

### 2. Setup

Run `lakeforge agent feature setup-plan --json` from the repository root and parse JSON for:
- `result`: "success" or error message
- `plan_file`: Absolute path to the created plan.md
- `feature_dir`: Absolute path to the feature directory

### 3. Load Context

Read:
- Feature spec (`spec.md`)
- Constitution (`.lakeforge/memory/constitution.md`) if it exists
- Plan template (`src/specify_cli/missions/databricks-lakehouse-buildout/templates/plan-template.md`)

### 4. Execute Plan Workflow

Fill the plan template with lakehouse-specific design:

#### Bronze Layer Design

| Source | Class | Connector | Landing Table | Validation |
|--------|-------|-----------|---------------|------------|
| [Source] | [S/M/L] | [Proper connector] | `dw_bronze.[table]` | [Checks] |

**Metadata Columns**:
- `_ingested_at TIMESTAMP`
- `_schema_version INT`

**Landing Strategy by Class**:
- Small (<1GB): Full refresh acceptable
- Medium (1-100GB): Incremental with watermark
- Large (>100GB): CDC or streaming required

#### Silver Layer Design

| Bronze Table | Silver Table | Mapping Rules | CDC Strategy |
|--------------|--------------|---------------|--------------|
| `dw_bronze.[source]` | `dw_silver.[entity]` | [Rules] | [Type-2 SCD] |

**Historization Columns**:
```sql
start_date TIMESTAMP,
end_date TIMESTAMP,
is_current BOOLEAN
```

#### Gold Layer Design

| Silver Table | Gold Table | Gold View | History Table |
|--------------|------------|-----------|---------------|
| `dw_silver.[entity]` | `gold_etl.t_[entity]` | `gold.v_[entity]` | `gold_hist.t_[entity]_hist` |

**View Structure** (explicit columns, no SELECT *):
```sql
CREATE OR REPLACE VIEW gold.v_[entity] AS
SELECT
    entity_id,
    entity_name,
    -- explicit column list
FROM gold_etl.t_[entity]
WHERE is_current = true
```

#### Quality Gates

| Gate ID | Location | Type | Rule | Action on Failure |
|---------|----------|------|------|-------------------|
| QG-001 | Bronze→Silver | [Type] | [Rule] | [Block/Quarantine/Alert] |
| QG-002 | Silver→Gold | [Type] | [Rule] | [Block/Quarantine/Alert] |

### 5. Phase 0: Outline & Research

Generate `research.md` with:
- Technology decisions and rationale
- Alternatives considered
- Best practices from Databricks documentation

### 6. Phase 1: Design & Contracts

Generate:
- `data-model.md` - Entity definitions with lakehouse columns
- `contracts/` - API schemas if applicable
- `quickstart.md` - Getting started guide

**Agent context update**:
- Run `{AGENT_SCRIPT}`
- These scripts detect which AI agent is in use
- Update the appropriate agent-specific context file
- Add lakehouse technologies from current plan (Delta Lake, Unity Catalog, connectors used)
- Preserve manual additions between markers

### 7. STOP and Report

**⚠️ CRITICAL: DO NOT proceed to task generation!**

Report:
- `plan.md` path
- `research.md` path (if generated)
- `data-model.md` path (if generated)
- Agent context file updated

**Next suggested command**: `/tasks` (user must invoke this explicitly)

## Layer-by-Layer Planning Structure

The implementation plan MUST include distinct sections for each layer:

### Bronze Layer Planning
- Source qualification (Class, Type, Schema, Sensitivity)
- Connector selection (proper connectors, no generic JDBC)
- Landing validation (distinct counts, time ranges)
- Metadata columns (_ingested_at, _schema_version)

### Silver Layer Planning
- Mapping definitions (source → target)
- CDC implementation (Type-2 SCD pattern)
- Historization strategy (start_date, end_date, is_current)
- Validation rules before Gold

### Gold Layer Planning
- View definitions (explicit columns, no SELECT *)
- Partition predicate mapping
- ACL configuration
- History table strategy (_hist suffix)

### Quality Gate Placement
Quality gates MUST be placed:
- After Bronze landing (basic validation)
- Before Gold promotion (business rules)

### Plan Validation Checklist

Before completing the plan, verify:
- [ ] Bronze landing design documented
- [ ] Silver transformation rules defined
- [ ] Gold view structure specified
- [ ] Quality gates placed between Silver and Gold
- [ ] Optimization strategy (Z-ordering, statistics) defined

## Key Rules

- Use absolute paths
- ERROR on gate failures or unresolved clarifications
- Fetch Databricks documentation before making architecture decisions
- Auto-correct deprecated terminology with explanation

---

## ⛔ MANDATORY STOP POINT

**This command is COMPLETE after generating planning artifacts.**

Do NOT:
- ❌ Generate `tasks.md`
- ❌ Create work package (WP) files
- ❌ Create `tasks/` subdirectories
- ❌ Proceed to implementation

The user will run `/tasks` when they are ready to generate work packages.
