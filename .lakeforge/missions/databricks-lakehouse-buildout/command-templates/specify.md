---
description: Create lakehouse feature specification with Bronze-first requirements.
---

# /specify - Create Lakehouse Feature Specification

**Version**: 0.11.0+

## 📍 WORKING DIRECTORY: Stay in MAIN repository

**IMPORTANT**: Specify works in the main repository. NO worktrees are created.

```bash
# Run from project root:
cd /path/to/project/root  # Your main repository

# All planning artifacts are created in main and committed:
# - lakeforge-specs/###-feature/spec.md → Created in main
# - Committed to main branch
# - NO worktrees created
```

**Worktrees are created later** during `/implement`, not during planning.

## Databricks Documentation Reference

**CRITICAL**: Before formulating discovery questions, gather current Databricks context:

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

**CRITICAL**: This mission enforces strict adherence to Databricks best practices throughout the ENTIRE workflow.

**Rules**:
1. **NEVER suggest approaches that contradict documented best practices**
   - Do NOT ask "Will you use DBFS for production data?" (docs recommend against it)
   - Do NOT suggest generic JDBC for specialized sources (use proper connectors)
   - Do NOT propose skipping Bronze layer or direct writes to Silver/Gold

2. **When user describes a non-compliant approach**:
   - REFUSE to proceed with the non-compliant pattern
   - EXPLAIN why it's not recommended (cite documentation)
   - OFFER the best-practice alternative

3. **Discovery questions must only suggest best-practice options**:
   - Good: "Which proper connector will you use for Salesforce data?"
   - Bad: "Will you use generic JDBC or a specialized connector?"

**Example enforcement**:
> User: "I want to land data directly in the Silver layer to save time"
> Agent: "I cannot proceed with direct Silver landing. Databricks best practices require Bronze-first data landing for audit trail and recovery. [docs.databricks.com/delta/best-practices.html] Let me help you design the Bronze landing instead - it can be streamlined for speed while maintaining architectural integrity."

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

## Discovery Gate (mandatory)

Before running any scripts or writing to disk you **must** conduct a structured discovery interview.

### Lakehouse Discovery Questions

In addition to standard discovery, ask these lakehouse-specific questions:

**Data Source Classification** - For each data source the user mentions, classify:
- **Class**: Small (<1GB), Medium (1-100GB), Large (>100GB)
- **Type**: Type-2 (tracks changes) or Non-Type-2
- **Schema**: Known/evolving/unknown
- **Sensitivity**: PII, Confidential, Public
- **Ingestion Strategy**: Full refresh (small only), Incremental, CDC

**Layer Strategy**:
- Will this feature touch Bronze, Silver, and/or Gold layers?
- Are there existing tables in any layer to integrate with?
- What is the expected data volume at each layer?

**Quality Gates**:
- What validation rules must pass before data moves to Gold?
- Are there specific business rules for data quality?
- How should failed records be handled (quarantine, reject, alert)?

**Example Discovery Question with Doc Link**:
> "Will you use Liquid Clustering for your Delta tables? [docs.databricks.com/delta/clustering.html]
> This is recommended for tables with high-cardinality columns."

### Scope Proportionality

- **Scope proportionality (CRITICAL)**: FIRST, gauge the inherent complexity of the request:
  - **Trivial/Test Features** (simple pipeline test, proof-of-concept): Ask 1-2 questions maximum, then proceed.
  - **Simple Features** (single source landing, basic transformation): Ask 2-3 questions covering purpose and data characteristics
  - **Complex Features** (multi-source integration, complex CDC): Ask 3-5 questions covering sources, transformation logic, quality rules
  - **Platform/Critical Features** (data governance, security, compliance): Full discovery with 5+ questions

- **User signals to reduce questioning**: If the user says "just testing", "quick prototype", "skip to next phase", "stop asking questions" - recognize this as a signal to minimize discovery and proceed with reasonable defaults.

- **First response rule**:
  - For TRIVIAL features: Ask ONE clarifying question about data source, then proceed directly to spec generation
  - For other features: Ask a single focused discovery question and end with `WAITING_FOR_DISCOVERY_INPUT`

### Discovery Requirements

1. Maintain a **Discovery Questions** table internally covering questions appropriate to the feature's complexity (1-2 for trivial, up to 5+ for complex). Track columns `#`, `Question`, `Why it matters`, and `Current insight`. Do **not** render this table to the user.
2. For trivial features, reasonable defaults are acceptable. Only probe if truly ambiguous.
3. When you have sufficient context for the feature's scope, paraphrase into an **Intent Summary** and confirm.
4. If user explicitly asks to skip questions, acknowledge and proceed with minimal discovery.

## Mission Selection

After completing discovery and confirming the Intent Summary, the mission is **databricks-lakehouse-buildout**.

Store `"databricks-lakehouse-buildout"` as the mission selection in your notes and include it in the spec output.

## Workflow (0.11.0+)

**Planning happens in main repository - NO worktree created!**

1. Creates `lakeforge-specs/###-feature/spec.md` directly in main repo
2. Automatically commits to main branch
3. No worktree created during specify

**Worktrees created later**: Use `lakeforge implement WP##` to create a workspace for each work package.

## Location

- Work in: **Main repository** (not a worktree)
- Creates: `lakeforge-specs/###-feature/spec.md`
- Commits to: `main` branch

## Outline

### 1. Generate a Friendly Feature Title

- Summarize the agreed intent into a short, descriptive title (aim for ≤7 words; avoid filler like "feature" or "thing").
- Use lakehouse terminology where appropriate (Bronze, Silver, Gold, CDC, etc.)
- Use the confirmed title to derive the kebab-case feature slug for the create-feature command.

### 2. Check Discovery Status

- If this is your first message or discovery questions remain unanswered, stay in the one-question loop, capture the user's response, update your internal table, and end with `WAITING_FOR_DISCOVERY_INPUT`.
- Only proceed once every discovery question has an explicit answer and the user has acknowledged the Intent Summary.

### 3. Create Feature

When discovery is complete and the intent summary, **title**, and **mission** are confirmed:

```bash
lakeforge agent feature create-feature "<slug>" --json
```

Parse the JSON for `feature`, `feature_dir`, and `result`.

### 4. Create meta.json

```json
{
  "feature_number": "<number>",
  "slug": "<full-slug>",
  "friendly_name": "<Friendly Title>",
  "mission": "databricks-lakehouse-buildout",
  "source_description": "$ARGUMENTS",
  "created_at": "<ISO timestamp>"
}
```

### 5. Generate Specification

Use the lakehouse spec template (`src/specify_cli/missions/databricks-lakehouse-buildout/templates/spec-template.md`) to generate:

- User Scenarios & Testing (mandatory)
- Data Sources section with source classification table
- Layer Requirements section for Bronze/Silver/Gold expectations
- Quality Gates section for validation rules
- Functional Requirements (mandatory)
- Success Criteria (mandatory)

### 6. Specification Quality Validation

Create checklist at `FEATURE_DIR/checklists/requirements.md`:

```markdown
# Specification Quality Checklist: [FEATURE NAME]

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: [DATE]
**Feature**: [Link to spec.md]

## Content Quality

- [ ] No implementation details (languages, frameworks, APIs)
- [ ] Focused on data pipeline value and business needs
- [ ] Written for non-technical stakeholders
- [ ] All mandatory sections completed

## Lakehouse-Specific Validation

- [ ] Data sources classified (Class, Type, Schema, Sensitivity)
- [ ] Layer requirements defined (Bronze, Silver, Gold)
- [ ] Quality gates specified
- [ ] Ingestion strategy documented for each source

## Requirement Completeness

- [ ] No [NEEDS CLARIFICATION] markers remain
- [ ] Requirements are testable and unambiguous
- [ ] Success criteria are measurable
- [ ] Success criteria are technology-agnostic
- [ ] All acceptance scenarios are defined
- [ ] Edge cases are identified
- [ ] Scope is clearly bounded

## Notes

- Items marked incomplete require spec updates before `/clarify` or `/plan`
```

### 7. Report Completion

Report completion with feature directory, spec file path, checklist results, and readiness for the next phase (`/clarify` or `/plan`).

## General Guidelines

### Quick Guidelines

- Focus on **WHAT** data needs to flow and **WHY**.
- Avoid HOW to implement (no specific SQL, PySpark code, etc.).
- Written for business stakeholders, not data engineers.
- DO NOT create any checklists that are embedded in the spec.

### Lakehouse-Specific Guidelines

- Always classify data sources before designing transformations
- Bronze layer: Raw data, no transformations
- Silver layer: Validated, historized, business-logic applied
- Gold layer: Customer-facing, aggregated, optimized
- Quality gates are NON-NEGOTIABLE between Silver and Gold

### For AI Generation

When creating this spec from a user prompt:

1. **Fetch Databricks docs first**: Use current terminology
2. **Auto-correct deprecated terms**: Explain the correction with doc link
3. **Make informed guesses**: Use context and lakehouse best practices
4. **Document assumptions**: Record defaults in the Assumptions section
5. **Limit clarifications**: Maximum 3 [NEEDS CLARIFICATION] markers
6. **Include doc links**: When referencing Databricks features

**Common areas needing clarification**:
- Data source connectivity (how to access source system)
- Data volume and growth expectations
- SLA requirements (how fresh must data be)
- Security/compliance requirements (PII handling)

**Reasonable defaults** (don't ask about these unless user signals otherwise):
- Use Delta Lake format
- Use Unity Catalog for governance
- Standard medallion architecture (Bronze → Silver → Gold)
- Type-2 SCD for dimension tracking
- Quality gates before Gold promotion
