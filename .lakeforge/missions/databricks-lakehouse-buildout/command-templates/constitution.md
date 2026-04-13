---
description: Optimize project constitution with Data Lakehouse principles (Bronze → Silver → Gold) so all Lakeforge commands follow lakehouse architecture.
---

You are a **Databricks expert** with deep knowledge of the latest best practices for lakehouse architecture, Delta Lake, Unity Catalog, and the Databricks platform. Apply this expertise throughout all recommendations and implementations. Always prefer native Databricks features and capabilities over third-party or custom solutions.

**Path reference rule:** When you mention directories or files, provide either the absolute path or a path relative to the project root (for example, `lakeforge-specs/<feature>/tasks/`). Never refer to a folder by name alone.

*Path: [templates/command-templates/lakeforge-lakehouse-buildout.md](templates/command-templates/lakeforge-lakehouse-buildout.md)*

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty). The user may provide a project name or additional context.

---

## What This Command Does

This command **writes lakehouse-optimized principles to `.lakeforge/memory/constitution.md`** so that all subsequent Lakeforge commands (`/specify`, `/plan`, `/tasks`, `/implement`) automatically enforce Data Lakehouse architecture patterns.

**What happens after you run this command:**
- `/specify` → Enforces Bronze-first data landing
- `/plan` → Validates layer transitions and quality gates
- `/tasks` → Organizes work by lakehouse phases
- `/implement` → Follows governance and quality gates

**Purpose**: Bootstrap constitution with battle-tested Data Lakehouse architectural standards for Databricks medallion architecture (Bronze → Silver → Gold).

**Location**: `.lakeforge/memory/constitution.md` (project root, not worktrees)

**Important**: The constitution is OPTIONAL. All Lakeforge commands work without it.

---

## When to Use This Command

- **Starting a new Databricks lakehouse project**: Apply foundational medallion architecture patterns
- **Standardizing an existing project**: Align with Data Lakehouse best practices
- **After `/constitution`**: Augment manual constitution with lakehouse principles

**You can run this**:
- From main repository root (recommended)
- From inside a feature worktree (will update main constitution)
- Multiple times (safe - intelligently merges with existing principles)

---

## Workflow Context

**Where it fits**: Optional utility command, not part of sequential workflow

**Typical sequence**:
1. Run `/constitution` (or skip if not needed)
2. Run `/lakeforge` to apply lakehouse principles
3. Proceed with `/specify` to start lakehouse feature work

**Constitution is optional**: All Lakeforge commands work without a constitution. This command is purely for convenience.

---

## Execution Outline

### Step 1: Load Existing Constitution

Load existing constitution at `.lakeforge/memory/constitution.md` (if it exists):
- Read all current principles and sections
- Note current version for incrementing
- Identify project name and governance rules

If no constitution exists, prepare to create a fresh one starting at version `1.0.0`.

### Step 2: Analyze for Conflicts and Optimization Opportunities

Compare existing principles against the 7 lakehouse principles (see Constitution Content section below). For each:

| Situation | Action |
|-----------|--------|
| **No conflict** (e.g., "Test-First Development") | PRESERVE existing principle alongside lakehouse principles |
| **Complementary** (e.g., "Specification-First") | PRESERVE and reference how it applies to lakehouse phases |
| **Contradiction** (e.g., "Direct table access allowed") | REPLACE with lakehouse principle, document in Sync Impact Report |
| **Partial overlap** (e.g., "Data validation required") | MERGE into lakehouse Quality Gates principle, enhance with layer-specific rules |
| **Missing** (principle doesn't exist) | ADD the lakehouse principle |

**Common conflicts to watch for:**
- "Skip staging for speed" → Conflicts with Bronze-First (REPLACE)
- "Direct database queries allowed" → Conflicts with View Abstraction (REPLACE)
- "Full refresh ingestion" → Conflicts with Incremental Strategy (MERGE - allow for small tables only)
- "Schema changes on demand" → Conflicts with Layer Separation (MERGE - add migration process)

### Step 3: Preserve Non-Data Principles

Keep principles that don't conflict with lakehouse architecture:
- Test-First / TDD requirements
- Specification-First development
- Code review requirements
- Security/compliance rules
- Documentation standards
- Any domain-specific principles

### Step 4: Construct the Merged Constitution

Structure the constitution as follows:

```markdown
<!-- Sync Impact Report
Version Change: [OLD_VERSION] → [NEW_VERSION] (Lakehouse Architecture Optimization)
Source: /lakeforge command

Lakehouse Principles Added:
- I. Bronze-First Data Landing
- II. Layer Separation and Progression
- III. Data Quality Gates (NON-NEGOTIABLE)
- IV. Incremental Ingestion Strategy
- V. Change Data Capture (CDC) and Historization
- VI. Optimization and Compaction
- VII. View Abstraction Layer

Preserved Principles (from existing constitution):
- [LIST ANY PRESERVED PRINCIPLES, e.g., "VIII. Test-First Development (was III)"]
- [e.g., "IX. Specification-First (was I)"]

Replaced/Merged Principles:
- [LIST ANY CONFLICTS RESOLVED, e.g., "Old 'Direct DB Access' replaced by 'VII. View Abstraction'"]
- [e.g., "Old 'Basic validation' merged into 'III. Data Quality Gates'"]

Removed Principles:
- [LIST ANY REMOVED, e.g., "None" or "'Skip staging' - incompatible with Bronze-First"]
-->

## Core Principles

### Lakehouse Architecture Principles (I-VII)
[The 7 lakehouse principles - always included]

### Preserved Principles (VIII+)
[Non-conflicting principles from existing constitution, renumbered]
```

### Step 5: Write the Optimized Constitution

Write the complete constitution to `.lakeforge/memory/constitution.md` (see Constitution Content section for full template).

### Step 6: Report Completion

Display summary of changes made (see Completion Report section for template).

---

## Constitution Content to Write

Write the following content to `.lakeforge/memory/constitution.md`:

```markdown
<!-- Sync Impact Report
Version Change: [OLD_VERSION] → [NEW_VERSION] (Lakehouse Architecture Optimization)
Source: /lakeforge command

Lakehouse Principles Added:
- I. Bronze-First Data Landing
- II. Layer Separation and Progression
- III. Data Quality Gates (NON-NEGOTIABLE)
- IV. Incremental Ingestion Strategy
- V. Change Data Capture (CDC) and Historization
- VI. Optimization and Compaction
- VII. View Abstraction Layer

Preserved Principles (from existing constitution):
- [LIST ANY PRESERVED PRINCIPLES, e.g., "VIII. Test-First Development (was III)"]
- [e.g., "IX. Specification-First (was I)"]

Replaced/Merged Principles:
- [LIST ANY CONFLICTS RESOLVED, e.g., "Old 'Direct DB Access' replaced by 'VII. View Abstraction'"]
- [e.g., "Old 'Basic validation' merged into 'III. Data Quality Gates'"]

Removed Principles:
- [LIST ANY REMOVED, e.g., "None" or "'Skip staging' - incompatible with Bronze-First"]
-->

# [PROJECT_NAME] Constitution

## Core Principles

### I. Bronze-First Data Landing
Every data source MUST first land in the Bronze layer as an exact copy of the source with zero transformations (aside from optional metadata columns like ingestion timestamp and schema version). Bronze serves as the audit trail and recovery point. Data SHALL NOT skip Bronze to land directly in Silver or Gold.

**Rules:**
- Source data MUST be copied exactly as-is (no business logic transformations)
- Add ingestion metadata: `_ingested_at` timestamp, `_schema_version` (start at 1.0)
- Use proper connectors for each source type (no generic JDBC for specialized sources)
- NEVER use DBFS blob storage for production data lakes
- Validate landing: distinct key counts, time ranges, file counts, average file sizes

### II. Layer Separation and Progression
Data MUST progress through layers in sequence: Bronze → Silver → Gold. Each layer has a distinct purpose. Skipping layers violates architectural integrity.

**Layer Definitions:**
- **Bronze**: Raw copy optimized for processing engine, mirrors source perfectly
- **Silver**: Validated, Type-2 historized, mappings applied, structured for Gold
- **Gold**: Customer-facing truth layer, optimized for consumption, accessed via views only

**Rules:**
- All transformations (mappings) MUST complete between Bronze and Silver
- Silver provides validation checkpoint before impacting customer-facing tables
- Gold consumers access views with 1-to-1 mappings, never tables directly

### III. Data Quality Gates (NON-NEGOTIABLE)
Data MUST NOT move to Gold without passing governance rules. Quality validation is mandatory. Failed records MUST be routed to review, not silently dropped.

**Governance Rule Types:**
- Range validation: Count between x and y
- Cross-field validation: Sum of price given rule A ≤ value x given rule Z
- Statistical validation: Standard deviation ≤ n
- Trend validation: Distinct values today ≥ distinct values yesterday
- Boundary validation: Max value ≤ n
- Temporal validation: Max timestamp ≤ today

**Rules:**
- Implement rules engine with scaffolding for easy rule addition
- Route failed records to centralized location for analyst review
- Schedule rule execution after Silver landing (preferred) or before Gold move

### IV. Incremental Ingestion Strategy
Data ingestion MUST support incremental patterns for production viability. Full dataset pulls are acceptable only for small, non-Type-2 sources.

**Ingestion Patterns:**
| Source Type | Strategy |
|-------------|----------|
| Small, non-Type-2 | Pull entire dataset each run, append timestamp |
| Type-2 | Pull records where update timestamp ≥ existing bronze timestamp |
| Large, non-Type-2 | Work with customer to identify strategy. CDC upsert in Bronze → Silver |

**Rules:**
- Demonstrate incremental capabilities (key value proposition)
- For snapshots: ingest ~60% initially, stage remaining for batch simulation
- Know source system load tolerance (avoid overwhelming during business hours)
- Document delete handling strategy (soft deletes typically preferred)

### V. Change Data Capture (CDC) and Historization
Silver layer MUST implement Type-2 SCD patterns for change tracking. Historical records MUST be marked with end_date or expired column. Gold splits current state from history.

**Rules:**
- Only current data is active in Silver; historical records marked expired
- Gold has paired tables: `table` (current only) and `table_hist` (full history)
- Omitting unnecessary versions in Gold improves performance and reduces costs

### VI. Optimization and Compaction
Delta Lake tables MUST be optimized according to established patterns. Compaction, statistics computation, MUST follow defined schedules.

**Rules:**
- Optimize with smaller clusters in off-hours
- Use separate small cluster for compaction (not ingestion cluster)
- Parallelize compaction across all tables
- Compute statistics: `ANALYZE TABLE <table> COMPUTE STATISTICS FOR COLUMNS key1, key2`

### VII. View Abstraction Layer
Customers MUST access Gold data through views, never tables directly. This provides flexibility for schema evolution and security.

**Rules:**
- Name every column explicitly (NO `SELECT *`)
- Map partition columns in predicate for partition pruning
- Alias every column for clear remapping path

## Technical Constraints

### Database Naming
| Layer | Database | Contents |
|-------|----------|----------|
| Bronze | `dw_bronze` | Raw data copies |
| Silver | `dw_silver` | Transformed, validated data |
| Gold (tables) | `gold_etl` | Persisted Gold tables |
| Gold (views) | `gold` | Consumer-facing views |
| Gold (history) | `gold_hist` | Historical data |

### Naming Conventions
| Convention | Example | Purpose |
|------------|---------|---------|
| Partition columns | `p_yyyymm` | Easy identification |
| Table prefix | `t_tablename` | Distinguish from views |
| History suffix | `_hist` | Identify history tables |

## Development Workflow

### Phase 1: Land & Solidify Bronze
- Qualify sources (Class, Type, Schema, Sensitivity)
- Internal review before implementation
- Ingest to Bronze as Delta
- Validate each landing
- Optimize and compute statistics

### Phase 2: Land & Solidify Silver
- Apply mappings (complete in notebooks)
- Complete validations
- Implement CDC
- Design Liquid Clustering

### Phase 3: Develop Gold Layer
- Create Gold databases
- Build table inventory
- Implement rules engine
- Handle rule failures
- Create views with proper structure
- Configure security (ACLs)

### Phase 4: Documentation and Close
- Never skip documentation
- Minimum 1 full day for documentation
- Document along the way

## Risk Escalation Triggers

> ⚠️ **Non-Type-2 Medium/Large Sources**: Must have explicit ingestion plan.

> ⚠️ **Unclear Mappings**: Project WILL NOT succeed without clear mappings.

## Governance

- Constitution MUST be reviewed at each phase transition
- Mapping changes require constitution amendment
- New data sources require qualification
- Version increments:
  - MAJOR: Layer architecture changes, breaking data model changes
  - MINOR: New sources, new governance rules, new quality gates
  - PATCH: Mapping refinements, documentation updates

**Version**: [VERSION] | **Ratified**: [DATE] | **Last Amended**: [TODAY]
```

---

## Version Management

- If no existing constitution: Start at version `1.0.0`
- If existing constitution: Increment based on changes:
  - First time adding lakehouse principles: MINOR bump (e.g., `1.0.0` → `1.1.0`)
  - Updating existing lakehouse principles: PATCH bump
  - Replacing incompatible principles: MAJOR bump

---

## Completion Report

After writing the constitution, report:

```
✅ Lakehouse-Optimized Constitution Written

**File**: .lakeforge/memory/constitution.md
**Version**: [OLD_VERSION] → [NEW_VERSION]
**Project**: [PROJECT_NAME]

📊 **Optimization Summary:**
- **Added**: 7 lakehouse principles (I-VII)
- **Preserved**: [N] existing principles (renumbered VIII+)
- **Replaced**: [N] conflicting principles
- **Merged**: [N] partial overlaps enhanced with lakehouse rules

🔄 **Changes Made:**
[If existing constitution was present, list key changes:]
- [e.g., "Preserved: Test-First Development → now Principle VIII"]
- [e.g., "Replaced: 'Direct queries allowed' → 'View Abstraction Layer'"]
- [e.g., "Merged: 'Data validation' enhanced with layer-specific Quality Gates"]

[If no existing constitution:]
- Fresh lakehouse constitution created with 7 core principles

📋 **Next Steps:**
1. Review the constitution at .lakeforge/memory/constitution.md
2. Verify preserved principles still align with lakehouse architecture
3. Run `/specify` to create your first lakehouse feature spec

💡 **How other commands use this:**
- `/specify` → Checks features follow Bronze-first landing
- `/plan` → Validates layer transitions in Constitution Check section
- `/tasks` → Organizes by lakehouse phases
- `/implement` → Enforces quality gates before layer transitions
```

---

## Examples

### Example 1: First run (no constitution exists)

```bash
$ lakeforge lakeforge

Applying Lakehouse Buildout principles to constitution...

Creating fresh constitution with 7 lakehouse principles...

✅ Lakehouse-Optimized Constitution Written

**File**: .lakeforge/memory/constitution.md
**Version**: 1.0.0
**Project**: [Detected from repo or user input]

📊 **Optimization Summary:**
- **Added**: 7 lakehouse principles (I-VII)
- **Preserved**: 0 existing principles
- **Replaced**: 0 conflicting principles
- **Merged**: 0 partial overlaps

🔄 **Changes Made:**
- Fresh lakehouse constitution created with 7 core principles

📋 **Next Steps:**
1. Review the constitution at .lakeforge/memory/constitution.md
2. Run `/specify` to create your first lakehouse feature spec
```

### Example 2: Augmenting existing constitution (complementary principles)

```bash
$ lakeforge lakeforge

Loading existing constitution...
Found version 1.0.0 with 3 existing principles

Analyzing for conflicts...
✓ Principle I (Test-First Development) - PRESERVE (complementary)
✓ Principle II (Specification-First) - PRESERVE (complementary)
✓ Principle III (Code Review Required) - PRESERVE (complementary)

Adding 7 lakehouse principles...

✅ Lakehouse-Optimized Constitution Written

**File**: .lakeforge/memory/constitution.md
**Version**: 1.0.0 → 1.1.0
**Project**: MyDataPlatform

📊 **Optimization Summary:**
- **Added**: 7 lakehouse principles (I-VII)
- **Preserved**: 3 existing principles (renumbered VIII-X)
- **Replaced**: 0 conflicting principles
- **Merged**: 0 partial overlaps

🔄 **Changes Made:**
- Preserved: Test-First Development → now Principle VIII
- Preserved: Specification-First → now Principle IX
- Preserved: Code Review Required → now Principle X
```

### Example 3: Resolving conflicts (replacing contradictory principles)

```bash
$ lakeforge lakeforge

Loading existing constitution...
Found version 2.3.0 with 5 existing principles

Analyzing for conflicts...
✓ Principle I (Test-First Development) - PRESERVE
⚠️ Principle II (Direct DB Access Allowed) - REPLACE with VII. View Abstraction
⚠️ Principle III (Skip Staging for Speed) - REPLACE with I. Bronze-First
✓ Principle IV (Data Validation Required) - MERGE into III. Quality Gates
✓ Principle V (Specification-First) - PRESERVE

Merging constitution...

✅ Lakehouse-Optimized Constitution Written

**File**: .lakeforge/memory/constitution.md
**Version**: 2.3.0 → 3.0.0 (MAJOR - incompatible principles replaced)
**Project**: MyDataPlatform

📊 **Optimization Summary:**
- **Added**: 7 lakehouse principles (I-VII)
- **Preserved**: 2 existing principles (renumbered VIII-IX)
- **Replaced**: 2 conflicting principles
- **Merged**: 1 partial overlap enhanced with lakehouse rules

🔄 **Changes Made:**
- Replaced: 'Direct DB Access Allowed' → 'VII. View Abstraction Layer'
- Replaced: 'Skip Staging for Speed' → 'I. Bronze-First Data Landing'
- Merged: 'Data Validation Required' enhanced with layer-specific Quality Gates (III)
- Preserved: Test-First Development → now Principle VIII
- Preserved: Specification-First → now Principle IX
```

---

## Error Handling

**Error: .lakeforge/ directory not found**
```
Error: .lakeforge/ directory not found
Initialize project first: lakeforge init .
```
**Solution**: Run `lakeforge init .` to initialize the project first.

**Error: Permission denied writing constitution file**
```
Error: Permission denied writing constitution file
Run: chmod u+w .lakeforge/memory/constitution.md
```
**Solution**: Fix file permissions with the suggested command.

**Error: Cannot parse existing constitution**
```
Warning: Could not parse existing constitution version or structure
Will create backup at .lakeforge/memory/constitution.md.backup
```
**Solution**: Constitution will be backed up and a fresh one created. Review backup to migrate custom principles.

**User cancellation (Ctrl+C)**
```
Cancelled by user
```
**Solution**: No changes made. Safe to re-run when ready.

---

## Technical Details

### Conflict Analysis Strategy

The command performs intelligent semantic analysis to merge lakehouse principles with existing constitution:

**Four merge strategies**:

1. **PRESERVE** (No conflict):
   - Existing principle doesn't conflict with lakehouse architecture
   - Example: "Test-First Development" alongside lakehouse principles
   - Action: Keep existing, renumber after lakehouse principles (VIII+)

2. **REPLACE** (Contradictory):
   - Existing principle directly conflicts with lakehouse requirement
   - Example: "Direct DB access" vs "View Abstraction Layer"
   - Action: Remove old principle, document in Sync Impact Report

3. **MERGE** (Partial overlap):
   - Existing principle overlaps but can be enhanced
   - Example: "Basic validation" enhanced into "Data Quality Gates"
   - Action: Incorporate into relevant lakehouse principle, document enhancement

4. **ADD** (Missing):
   - No existing principle covers this lakehouse requirement
   - Action: Add lakehouse principle

### Constitution Structure

After applying Lakehouse Buildout:

```markdown
<!-- Sync Impact Report (comments at top) -->

# [PROJECT_NAME] Constitution

## Core Principles

### I. Bronze-First Data Landing
[Full principle text...]

### II. Layer Separation and Progression
[Full principle text...]

[... III-VII ...]

### VIII. [Preserved Principle Name]
[Preserved from original constitution...]

## Technical Constraints
[Database naming, conventions...]

## Development Workflow
[Phase-based approach...]

## Risk Escalation Triggers
[Warning signs...]

## Governance
[Version management, amendment process...]
```

### Version Management Rules

Version numbers follow semantic versioning:

- **MAJOR** (X.0.0): Incompatible principles replaced, breaking changes
- **MINOR** (0.X.0): First-time lakehouse addition, new complementary principles
- **PATCH** (0.0.X): Documentation updates, mapping refinements

**Examples**:
- Fresh constitution: `1.0.0`
- First lakehouse addition: `1.0.0` → `1.1.0` (MINOR)
- Replacing conflicting principles: `2.3.0` → `3.0.0` (MAJOR)
- Updating existing lakehouse principles: `1.1.0` → `1.1.1` (PATCH)

---

## Success Criteria

- ✅ Constitution file created or updated at `.lakeforge/memory/constitution.md`
- ✅ All 7 lakehouse principles (I-VII) added to "Core Principles" section
- ✅ Existing non-conflicting principles preserved and renumbered (VIII+)
- ✅ Conflicts resolved (contradictory replaced, complementary preserved, partial overlaps merged)
- ✅ Sync Impact Report added as comment at top of file
- ✅ Version number incremented appropriately
- ✅ Completion report displayed with summary of changes