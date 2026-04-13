---
description: Review lakehouse implementation against 7 principles and layer requirements.
---

# /review - Lakehouse Architecture Compliance Review

## Databricks Documentation Reference

**Before reviewing**, optionally fetch current best practices:

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

**CRITICAL**: Reviews MUST reject implementations that violate Databricks best practices.

**Zero-tolerance violations** (automatic rejection):
- Transformations in Bronze layer
- Direct writes to Silver/Gold (skipping layers)
- DBFS blob storage for production data
- Generic JDBC for specialized sources
- SELECT * in Gold views
- Missing quality gates between Silver and Gold
- Missing historization columns in Silver (for Type-2 sources)

**Review enforcement**:
1. Any violation of documented best practices → REQUEST CHANGES (not warnings)
2. Provide doc link explaining the correct approach
3. Implementation must be fixed before approval

---

## Review Command

**IMPORTANT**: After running the command below, you'll see a LONG work package prompt (~1000+ lines).

**You MUST scroll to the BOTTOM** to see the completion commands!

Run this command to get the work package prompt and review instructions:

```bash
lakeforge agent workflow review $ARGUMENTS --agent <your-name>
```

**CRITICAL**: You MUST provide `--agent <your-name>` to track who is reviewing!

If no WP ID is provided, it will automatically find the first work package with `lane: "for_review"` and move it to "doing" for you.

---

## 7 Lakehouse Principles Compliance Checklist

Every implementation MUST be validated against these principles:

### Principle I: Bronze-First Data Landing

- [ ] All data sources land in Bronze first
- [ ] No transformations applied in Bronze layer
- [ ] Metadata columns present (`_ingested_at`, `_schema_version`)
- [ ] Proper connectors used (not generic JDBC for specialized sources)
- [ ] DBFS blob storage NOT used for production data

**Red flags to look for**:
- `SELECT * ... WHERE ...` in Bronze (filtering = transformation)
- Missing `_ingested_at` column
- Generic JDBC connector for Salesforce, SAP, etc.
- Data landing directly in DBFS `/mnt/` paths

### Principle II: Layer Separation and Progression

- [ ] Data progresses Bronze → Silver → Gold (never skips)
- [ ] Bronze mirrors source perfectly (no transformations)
- [ ] Silver contains validated, historized data
- [ ] Gold is customer-facing only (views, not tables)

**Red flags to look for**:
- Bronze tables with transformation logic
- Direct writes from Bronze to Gold (skipping Silver)
- Gold tables exposed directly (not via views)

### Principle III: Data Quality Gates (NON-NEGOTIABLE)

- [ ] Quality gates exist between Silver and Gold
- [ ] Failed records routed to review (not silently dropped)
- [ ] Governance rules implemented:
  - [ ] Range validation (values within expected bounds)
  - [ ] Cross-field validation (relationships hold)
  - [ ] Statistical validation (distributions match expectations)
  - [ ] Trend validation (time-based consistency)

**Red flags to look for**:
- No validation logic between Silver and Gold
- `WHERE` clauses that silently filter out bad records
- Missing quarantine tables for failed records
- No alerting on quality gate failures

### Principle IV: Incremental Ingestion Strategy

- [ ] No full dataset pulls for large (>1GB) sources
- [ ] Incremental pattern documented and implemented
- [ ] Source system load tolerance considered
- [ ] Watermark or CDC used for change detection

**Red flags to look for**:
- `SELECT * FROM source` without date filters for large tables
- Full table overwrites instead of incremental updates
- No watermark tracking for incremental loads

### Principle V: CDC and Historization

- [ ] Type-2 SCD implemented in Silver
- [ ] Historical records marked with `end_date`/expired
- [ ] `is_current` flag present for convenience
- [ ] Gold has paired tables: `table` (current) and `table_hist` (history)

**Red flags to look for**:
- Missing `start_date`, `end_date`, `is_current` columns
- Type-1 SCD (overwriting) instead of Type-2 (historization)
- No history preservation for dimension changes
- Missing `_hist` tables in Gold layer

### Principle VI: Optimization and Compaction

- [ ] Tables optimized with Z-ordering where appropriate
- [ ] Statistics computed for key columns
- [ ] Auto-optimize NOT applied to Z-ordered tables
- [ ] Compaction scheduled appropriately

**Red flags to look for**:
- Large tables without Z-ordering on filter columns
- Missing `ANALYZE TABLE` or statistics computation
- Auto-optimize enabled on tables with explicit Z-ordering
- No compaction schedule for streaming tables

### Principle VII: View Abstraction Layer

- [ ] Customers access Gold via views only
- [ ] Every column explicitly named (no `SELECT *`)
- [ ] Partition columns mapped in predicates
- [ ] All columns aliased appropriately

**Red flags to look for**:
- Direct table access granted to customers
- `SELECT *` in view definitions
- Missing partition predicate pushdown
- Ambiguous or poorly named columns

---

## Layer-Specific Validation

### Bronze Layer Review

| Check | Pass/Fail | Notes |
|-------|-----------|-------|
| Raw data only (no transforms) | | |
| Metadata columns present (`_ingested_at`, `_schema_version`) | | |
| Proper connector used | | |
| Landing validation implemented | | |
| No DBFS for production data | | |

### Silver Layer Review

| Check | Pass/Fail | Notes |
|-------|-----------|-------|
| CDC implemented (MERGE INTO) | | |
| Historization columns present (`start_date`, `end_date`, `is_current`) | | |
| Validation before Gold | | |
| Mappings documented | | |
| All transformations complete (not deferred) | | |

### Gold Layer Review

| Check | Pass/Fail | Notes |
|-------|-----------|-------|
| Views used (not direct table access) | | |
| Explicit column naming (no SELECT *) | | |
| ACLs configured | | |
| Optimization applied (Z-ordering, statistics) | | |
| History tables created (`_hist` suffix) | | |

---

## Dependency Checks (required)

- **dependency_check**: If the WP frontmatter lists `dependencies`, confirm each dependency WP is merged to main before you review this WP.
- **dependent_check**: Identify any WPs that list this WP as a dependency and note their current lanes.
- **rebase_warning**: If you request changes AND any dependents exist, warn those agents to rebase.
- **verify_instruction**: Confirm dependency declarations match actual code coupling.

---

## Review Outcome

Based on the 7 principles checklist results:

### ✅ APPROVE
All 7 principles satisfied, layer checks pass:
```bash
lakeforge agent tasks move-task WP## --to done --note "Review passed: <summary>"
```

### ❌ REQUEST CHANGES
Document specific failures and required fixes:
1. Write feedback to the temp file path shown in the prompt
2. Run:
```bash
lakeforge agent tasks move-task WP## --to planned --review-feedback-file <temp-file-path>
```

**Feedback format**:
```markdown
## Review Feedback: WP##

### Principle Violations
- **Principle III**: Quality gates missing between Silver and Gold
  - Location: `silver_to_gold.py` line 45
  - Issue: No validation before Gold insert
  - Required: Add quality gate check

### Layer-Specific Issues
- **Gold Layer**: SELECT * used in view definition
  - Location: `create_views.sql` line 12
  - Required: Explicit column list

### Required Changes
1. Add quality gate before Gold insert
2. Replace SELECT * with explicit columns
3. Add quarantine table for failed records
```

### ⚠️ ESCALATE
Architectural concerns requiring stakeholder input:
- Fundamental design flaws that affect data integrity
- Security concerns (PII exposure, missing ACLs)
- Performance concerns at scale

---

**After reviewing, scroll to the bottom and run ONE of the completion commands above!**

**The Python script handles all file updates automatically - no manual editing required!**
