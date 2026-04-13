---
description: Execute lakehouse implementation with Databricks best practices from current docs.
---

# /implement - Lakehouse Implementation

## ⚠️ CRITICAL: Working Directory Requirement

**After running `lakeforge implement WP##`, you MUST:**

1. **Run the cd command shown in the output** - e.g., `cd .worktrees/###-feature-WP##/`
2. **ALL file operations happen in this directory** - Read, Write, Edit tools must target files in the workspace
3. **NEVER write deliverable files to the main repository** - This is a critical workflow error

---

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

**CRITICAL**: This mission enforces strict adherence to Databricks best practices. You MUST NOT implement patterns that contradict documented recommendations.

**Before writing ANY code**:
1. Fetch relevant documentation from the sitemap
2. Verify the approach aligns with documented best practices
3. If the WP describes a non-compliant pattern, STOP and flag it

**Non-negotiable best practices**:
- Bronze layer: NO transformations, raw data only with metadata columns
- Connectors: Use proper connectors (NOT generic JDBC for specialized sources)
- Storage: Delta Lake format (NOT Parquet/CSV for lakehouse tables)
- Production data: Unity Catalog managed tables (NOT DBFS blob storage)
- Gold access: Views only (NOT direct table access for customers)
- CDC: Type-2 SCD with historization columns (NOT overwrites)

**If WP requests non-compliant implementation**:
> "I cannot implement [non-compliant pattern] as it contradicts Databricks best practices. The documented recommendation is [best practice]. [doc link] I will implement the best-practice approach instead."

---

### Step 1: Fetch Documentation

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
```

### Step 2: Apply Documentation to Implementation

After fetching, apply the patterns from documentation. Common patterns:

#### Bronze Layer Patterns

- Use Auto Loader for incremental file ingestion
- Add metadata columns: `_ingested_at = current_timestamp()`, `_schema_version = 1`
- Use `COPY INTO` for batch file ingestion
- NEVER apply transformations - raw data only
- Use proper connectors (not generic JDBC for specialized sources)
- DBFS blob storage NOT for production data

**Example Bronze landing**:
```sql
-- Auto Loader for incremental ingestion
CREATE OR REFRESH STREAMING TABLE dw_bronze.raw_customers
AS SELECT
    *,
    current_timestamp() AS _ingested_at,
    1 AS _schema_version
FROM cloud_files('/path/to/source', 'json')
```

#### Silver Layer Patterns

- Implement CDC with `MERGE INTO` statement
- Add historization columns:
  ```sql
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  is_current BOOLEAN
  ```
- Use Type-2 SCD pattern for change tracking
- Validate data before moving to Gold
- All transformations complete in Silver (not Gold)

**Example Silver transformation with CDC**:
```sql
MERGE INTO dw_silver.customers AS target
USING (
    SELECT * FROM dw_bronze.raw_customers
    WHERE _ingested_at > (SELECT MAX(_ingested_at) FROM dw_silver.customers)
) AS source
ON target.customer_id = source.customer_id AND target.is_current = true
WHEN MATCHED AND (target.name != source.name OR target.email != source.email) THEN
    UPDATE SET end_date = current_timestamp(), is_current = false
WHEN NOT MATCHED THEN
    INSERT (customer_id, name, email, start_date, end_date, is_current)
    VALUES (source.customer_id, source.name, source.email, current_timestamp(), NULL, true)
```

#### Gold Layer Patterns

- Create views, not direct table access:
  ```sql
  CREATE OR REPLACE VIEW gold.v_customer AS
  SELECT
      customer_id,
      customer_name,
      email
  FROM gold_etl.t_customer
  WHERE is_current = true
  ```
- Explicitly name ALL columns (no `SELECT *`)
- Map partition columns in predicates
- Configure proper ACLs
- Apply Z-ordering on high-cardinality filter columns

**Example Gold view**:
```sql
CREATE OR REPLACE VIEW gold.v_customers AS
SELECT
    customer_id,
    customer_name,
    email,
    created_date,
    region
FROM gold_etl.t_customers
WHERE is_current = true
```

### Fallback if WebSearch Fails

If documentation is unavailable:
1. Display warning: "Databricks documentation unavailable - using built-in patterns"
2. Proceed with patterns above
3. Note in activity log that docs were unavailable

---

## Implementation Command

**IMPORTANT**: After running the command below, you'll see a LONG work package prompt (~1000+ lines).

**You MUST scroll to the BOTTOM** to see the completion command!

Run this command to get the work package prompt and implementation instructions:

```bash
lakeforge agent workflow implement $ARGUMENTS --agent <your-name>
```

**CRITICAL**: You MUST provide `--agent <your-name>` to track who is implementing!

If no WP ID is provided, it will automatically find the first work package with `lane: "planned"` and move it to "doing" for you.

---

## Implementation Checkpoints

After each layer implementation, verify against the 7 Lakehouse Principles:

### Bronze Checkpoint

- [ ] No transformations applied (raw data only)
- [ ] Metadata columns present (`_ingested_at`, `_schema_version`)
- [ ] Proper connector used (not generic JDBC)
- [ ] DBFS blob storage NOT used for production data
- [ ] Landing validation passes (distinct counts, time ranges)

### Silver Checkpoint

- [ ] Mappings complete and documented
- [ ] CDC implemented (Type-2 SCD with start_date, end_date, is_current)
- [ ] Historization columns present
- [ ] Validation before Gold (data quality checks)
- [ ] All transformations complete (not deferred to Gold)

### Gold Checkpoint

- [ ] Views created (not direct table access)
- [ ] Explicit column naming (no SELECT *)
- [ ] ACLs configured
- [ ] Optimization applied (Z-ordering, statistics)
- [ ] History tables created with `_hist` suffix

### Quality Gate Checkpoint

- [ ] Quality gates operational between Silver and Gold
- [ ] Failed records routed to quarantine (not silently dropped)
- [ ] Governance rules implemented:
  - [ ] Range validation
  - [ ] Cross-field validation
  - [ ] Statistical validation
  - [ ] Trend validation

---

## Commit Workflow

**BEFORE moving to for_review**, you MUST commit your implementation:

```bash
cd .worktrees/###-feature-WP##/
git add -A
git commit -m "feat(WP##): <describe your implementation>"
```

**Then move to review:**
```bash
lakeforge agent tasks move-task WP## --to for_review --note "Ready for review: <summary>"
```

**Why this matters:**
- `move-task` validates that your worktree has commits beyond main
- Uncommitted changes will block the move to for_review
- This prevents lost work and ensures reviewers see complete implementations

---

**The Python script handles all file updates automatically - no manual editing required!**

**NOTE**: If `/status` shows your WP in "doing" after you moved it to "for_review", don't panic - a reviewer may have moved it back (changes requested), or there's a sync delay. Focus on your WP.
