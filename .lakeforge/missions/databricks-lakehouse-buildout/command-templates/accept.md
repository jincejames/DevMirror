---
description: Verify lakehouse architecture compliance for feature acceptance.
---

# /accept - Lakehouse Feature Acceptance

**Version**: 0.11.0+
**Purpose**: Validate all work packages are complete and lakehouse architecture is compliant.

## 📍 WORKING DIRECTORY: Run from MAIN repository

**IMPORTANT**: Accept runs from the main repository root, NOT from a WP worktree.

```bash
# If you're in a worktree, return to main first:
cd $(git rev-parse --show-toplevel)

# Then run accept:
lakeforge accept
```

## Best Practices Enforcement (STRICT)

**CRITICAL**: Acceptance MUST verify strict compliance with Databricks best practices.

**Acceptance blockers** (cannot accept if any present):
- ANY violation of the 7 lakehouse principles
- Patterns contradicting documented best practices
- Missing Bronze layer for any data source
- Missing quality gates before Gold
- Direct table access instead of views in Gold
- Non-Delta formats for lakehouse tables

**Acceptance rule**: If implementation contains ANY best-practice violation, it MUST be rejected regardless of functional correctness.

---

## Databricks Documentation Reference

**Before accepting**, verify implementation against current best practices:
- **Sitemap**: https://docs.databricks.com/llms.txt

```
WebSearch: https://docs.databricks.com/llms.txt
Query: "Search for: medallion architecture best practices production checklist"
```

---

## User Input

```text
$ARGUMENTS
```

You **MUST** consider the user input before proceeding (if not empty).

---

## Lakehouse Architecture Final Validation

### Pre-Acceptance Checklist

**Layer Completion**:
- [ ] Bronze layer: All sources landing correctly
- [ ] Silver layer: CDC and historization complete
- [ ] Gold layer: Views and optimization in place
- [ ] Documentation: Complete and accurate

**Architecture Compliance**:
- [ ] Bronze-first landing verified (no direct Silver/Gold writes)
- [ ] Layer progression verified (Bronze → Silver → Gold)
- [ ] Quality gates operational between Silver and Gold
- [ ] Incremental ingestion working (no full pulls for large sources)
- [ ] CDC and historization validated (Type-2 SCD)
- [ ] Optimization applied (Z-ordering, statistics)
- [ ] View abstraction in place (no direct table access)

**Quality Assurance**:
- [ ] All quality gates passing
- [ ] Failed records properly routed to quarantine
- [ ] No data quality regressions
- [ ] Alerting configured for gate failures

---

## Database Naming Verification

Verify naming conventions match standard lakehouse patterns:

| Layer | Expected Database | Actual | Status |
|-------|-------------------|--------|--------|
| Bronze | `dw_bronze` | | [ ] Verified |
| Silver | `dw_silver` | | [ ] Verified |
| Gold (tables) | `gold_etl` | | [ ] Verified |
| Gold (views) | `gold` | | [ ] Verified |
| Gold (history) | `gold_hist` | | [ ] Verified |

---

## 7 Lakehouse Principles Final Verification

| Principle | Requirement | Verified |
|-----------|-------------|----------|
| I. Bronze-First | All sources land in Bronze first, no transformations | [ ] |
| II. Layer Separation | Data progresses Bronze → Silver → Gold (no skips) | [ ] |
| III. Quality Gates | Validation before Gold promotion (NON-NEGOTIABLE) | [ ] |
| IV. Incremental | No full pulls for large sources | [ ] |
| V. CDC/Historization | Type-2 SCD in Silver, `_hist` tables in Gold | [ ] |
| VI. Optimization | Z-ordering on filter columns, statistics computed | [ ] |
| VII. View Abstraction | Customers access views only, explicit columns | [ ] |

---

## Acceptance Criteria Sign-Off

| Criterion | Met? | Evidence |
|-----------|------|----------|
| All 7 lakehouse principles satisfied | | |
| All WPs in "done" lane | | |
| Documentation complete | | |
| Quality gates operational | | |
| Security (ACLs) configured | | |
| Optimization applied | | |
| No data quality regressions | | |

---

## Discovery (mandatory)

Before running the acceptance workflow, gather the following:

1. **Feature slug** (e.g., `005-lakehouse-feature`). If omitted, detect automatically.
2. **Acceptance mode**:
   - `pr` when the feature will merge via hosted pull request.
   - `local` when the feature will merge locally without a PR.
   - `checklist` to run the readiness checklist without committing.
3. **Validation commands executed** (tests/builds). Collect each command verbatim.
4. **Acceptance actor** (optional, defaults to the current agent name).

Ask one focused question per item and confirm the summary before continuing.

---

## Execution Plan

1. Compile the acceptance options into an argument list:
   - Always include `--actor "__AGENT__"`.
   - Append `--feature "<slug>"` when the user supplied a slug.
   - Append `--mode <mode>` (`pr`, `local`, or `checklist`).
   - Append `--test "<command>"` for each validation command provided.

2. Run `{SCRIPT}` (the CLI wrapper) with the assembled arguments **and** `--json`.

3. Parse the JSON response for:
   - `summary.ok` (boolean)
   - `summary.outstanding` categories
   - `instructions` (merge steps)
   - `cleanup_instructions`

4. Present the outcome with lakehouse-specific context.

---

## Acceptance Decision

### ✅ ACCEPT
All criteria met, lakehouse feature ready for production:
- All 7 principles satisfied
- All WPs in "done" lane
- Documentation complete
- Quality gates operational
- Security configured
- Optimization applied

**Post-acceptance steps**:
1. Mark all WPs as "done" in kanban
2. Update feature documentation
3. Archive or close feature branch

### ❌ REJECT
Document failures, return to implementation:
- List which principles failed
- Identify specific WPs needing rework
- Provide clear remediation steps

### ⚠️ CONDITIONAL ACCEPT
Minor issues, document for follow-up:
- Non-blocking issues that can be addressed post-merge
- Performance optimizations that can be incremental
- Documentation gaps that don't affect functionality

---

## Post-Acceptance

Upon acceptance:
1. Mark all WPs as "done" in kanban
2. Update feature documentation with final architecture
3. Archive or close feature branch
4. Update data catalog with new tables/views
5. Configure monitoring for quality gates
6. Hand off to operations team

---

## Error Handling

- If the command fails or returns invalid JSON, report the failure and request user guidance.
- When outstanding issues exist, do **not** attempt to force acceptance—return the checklist and prompt the user to fix the blockers.
- For lakehouse-specific failures, reference the 7 principles for remediation guidance.
