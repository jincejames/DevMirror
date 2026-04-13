# Implementation Plan: [FEATURE]

**Branch**: `[###-feature-name]` | **Date**: [DATE] | **Spec**: [link]
**Input**: Feature specification from `/lakeforge-specs/[###-feature-name]/spec.md`

**Note**: This template is filled in by the `/plan` command. See `src/specify_cli/missions/databricks-lakehouse-buildout/command-templates/plan.md` for the execution workflow.

The planner will not begin until all planning questions have been answered—capture those answers in this document before progressing to later phases.

## Summary

[Extract from feature spec: primary requirement + technical approach from research]

## Technical Context

<!--
  ACTION REQUIRED: Replace the content in this section with the technical details
  for the project. The structure here is presented in advisory capacity to guide
  the iteration process.

  If multiple developers/agents will work on this feature, add a "Parallel Work
  Organization" section below showing the dependency graph and agent assignments.
-->

**Language/Version**: [e.g., Python 3.11, Scala 2.12, SQL or NEEDS CLARIFICATION]
**Primary Dependencies**: [e.g., PySpark, Delta Lake, Unity Catalog or NEEDS CLARIFICATION]
**Storage**: [e.g., Delta Lake on ADLS, S3, DBFS or NEEDS CLARIFICATION]
**Testing**: [e.g., pytest, Great Expectations, custom validators or NEEDS CLARIFICATION]
**Target Platform**: [e.g., Databricks Runtime 14.x, Serverless or NEEDS CLARIFICATION]
**Performance Goals**: [e.g., 1M rows/hour Bronze, <15min Gold refresh or NEEDS CLARIFICATION]
**Constraints**: [e.g., Must use Unity Catalog, no DBFS for production or NEEDS CLARIFICATION]
**Scale/Scope**: [e.g., 10 sources, 50 tables, 1TB/day or NEEDS CLARIFICATION]

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

[Gates determined based on constitution file]

## Lakehouse Architecture Design

### Bronze Layer Design

| Source | Class | Connector | Landing Table | Validation |
|--------|-------|-----------|---------------|------------|
| [Source 1] | [S/M/L] | [Proper connector] | `dw_bronze.[table]` | [Distinct counts, time ranges] |

**Metadata Columns**:
- `_ingested_at TIMESTAMP` - When data was ingested
- `_schema_version INT` - Schema version for evolution tracking

**Landing Strategy**:
- Small (<1GB): Full refresh acceptable
- Medium (1-100GB): Incremental with watermark
- Large (>100GB): CDC or streaming required

**Connector Selection** (use proper connectors, not generic JDBC):
- Salesforce → Salesforce connector
- SAP → SAP connector
- SQL Server → SQL Server connector with CDC
- Files → Auto Loader

### Silver Layer Design

| Bronze Table | Silver Table | Mapping Rules | CDC Strategy |
|--------------|--------------|---------------|--------------|
| `dw_bronze.[source]` | `dw_silver.[entity]` | [Transformation rules] | [Type-2 SCD] |

**Historization Columns**:
```sql
start_date TIMESTAMP,  -- When this version became current
end_date TIMESTAMP,    -- When this version was superseded (NULL if current)
is_current BOOLEAN     -- Convenience flag for current record
```

**Partitioning Strategy**: [e.g., by date, by region]

**Validation Rules** (must pass before Gold):
- [ ] Range validation: [specific rules]
- [ ] Cross-field validation: [specific rules]
- [ ] Referential integrity: [specific rules]

### Gold Layer Design

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

**Optimization**:
- Z-ordering columns: [high-cardinality filter columns]
- Statistics on: [key join/filter columns]
- Auto-optimize: OFF for Z-ordered tables

**Security (ACLs)**:
- Gold views: [who can access]
- Gold tables: [restricted to ETL service principal]
- Bronze/Silver: [restricted to data engineering team]

### Quality Gates

| Gate ID | Location | Type | Rule | Action on Failure |
|---------|----------|------|------|-------------------|
| QG-001 | Bronze→Silver | [Type] | [Rule] | [Block/Quarantine/Alert] |
| QG-002 | Silver→Gold | [Type] | [Rule] | [Block/Quarantine/Alert] |

**Failed Record Handling**:
- Quarantine table: `dw_silver.[entity]_quarantine`
- Alert mechanism: [Slack, email, PagerDuty]
- Review process: [Manual review, auto-retry]

## Project Structure

### Documentation (this feature)

```
lakeforge-specs/[###-feature]/
├── plan.md              # This file (/plan command output)
├── research.md          # Phase 0 output (/plan command)
├── data-model.md        # Phase 1 output (/plan command)
├── quickstart.md        # Phase 1 output (/plan command)
├── contracts/           # Phase 1 output (/plan command)
└── tasks.md             # Phase 2 output (/tasks command)
```

### Pipeline Code (repository root)

```
pipelines/
├── bronze/
│   ├── landing_[source1].py
│   └── landing_[source2].py
├── silver/
│   ├── transform_[entity1].py
│   └── transform_[entity2].py
├── gold/
│   ├── deliver_[entity1].py
│   └── views/
│       └── create_views.sql
└── quality/
    ├── gates/
    └── quarantine/

tests/
├── integration/
│   ├── test_bronze_landing.py
│   ├── test_silver_transform.py
│   └── test_gold_delivery.py
└── quality/
    └── test_quality_gates.py
```

## Development Workflow

### Phase 1: Land & Solidify Bronze

1. **Qualify sources** - Classify each source (Class, Type, Schema, Sensitivity)
2. **Internal review** - Validate connector selection before implementation
3. **Ingest to Bronze** - Land as Delta with metadata columns
4. **Validate landing** - Run distinct counts, time range checks
5. **Optimize** - Compute statistics, configure retention

### Phase 2: Land & Solidify Silver

1. **Apply mappings** - Transform Bronze to Silver (complete in notebooks first)
2. **Complete validations** - All transformation rules verified
3. **Implement CDC** - Type-2 SCD with start_date, end_date, is_current
4. **Design partitioning** - Choose partition strategy
5. **Implement Z-ordering** - Optimize for query patterns

### Phase 3: Develop Gold Layer

1. **Create Gold databases** - `gold_etl`, `gold`, `gold_hist`
2. **Build table inventory** - All Gold tables with explicit schema
3. **Implement quality gates** - Rules engine before Gold promotion
4. **Handle failures** - Quarantine tables, alerting
5. **Create views** - Explicit columns, proper structure
6. **Configure security** - ACLs for each layer

### Phase 4: Documentation and Close

1. **Technical documentation** - Architecture, data flows, troubleshooting
2. **Runbook** - Operations procedures, incident response
3. **Data dictionary** - Column definitions, business rules
4. **Never skip documentation** - Minimum 1 full day allocated

## Parallel Work Analysis

*Include this section if multiple developers/agents will implement this feature*

### Dependency Graph

```
Phase 1 (Bronze) - Sequential by source
  └─→ Phase 2 (Silver) - Can parallelize independent entities
       └─→ Phase 3 (Gold) - Sequential (quality gates must be in place)
            └─→ Phase 4 (Docs) - Can parallelize by doc type

Within Phase 1:
  Source A Bronze ──┐
  Source B Bronze ──┼─→ Phase 2 (when all sources ready)
  Source C Bronze ──┘
```

### Work Distribution

- **Sequential work**: Bronze landing (each source depends on connector setup)
- **Parallel streams**: Silver transformations (independent entities can be done simultaneously)
- **Integration points**: Quality gates must be complete before any Gold work

### Coordination Points

- **Bronze complete**: All sources landing, validated
- **Silver complete**: All transformations, CDC, validations
- **Gold ready**: Quality gates operational, views created
- **Documentation sync**: Final review before close
