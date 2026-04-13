# Feature Specification: [FEATURE NAME]
<!-- Replace [FEATURE NAME] with the confirmed friendly title generated during /specify. -->

**Feature Branch**: `[###-feature-name]`
**Created**: [DATE]
**Status**: Draft
**Input**: User description: "$ARGUMENTS"

## User Scenarios & Testing *(mandatory)*

<!--
  IMPORTANT: User stories should be PRIORITIZED as user journeys ordered by importance.
  Each user story/journey must be INDEPENDENTLY TESTABLE - meaning if you implement just ONE of them,
  you should still have a viable MVP (Minimum Viable Product) that delivers value.

  Assign priorities (P1, P2, P3, etc.) to each story, where P1 is the most critical.
  Think of each story as a standalone slice of functionality that can be:
  - Developed independently
  - Tested independently
  - Deployed independently
  - Demonstrated to users independently
-->

### User Story 1 - [Brief Title] (Priority: P1)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently - e.g., "Can be fully tested by [specific action] and delivers [specific value]"]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]
2. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### User Story 2 - [Brief Title] (Priority: P2)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

### User Story 3 - [Brief Title] (Priority: P3)

[Describe this user journey in plain language]

**Why this priority**: [Explain the value and why it has this priority level]

**Independent Test**: [Describe how this can be tested independently]

**Acceptance Scenarios**:

1. **Given** [initial state], **When** [action], **Then** [expected outcome]

---

[Add more user stories as needed, each with an assigned priority]

### Edge Cases

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right edge cases.
-->

- What happens when [boundary condition]?
- How does system handle [error scenario]?

## Data Sources *(lakehouse-specific)*

### Source Classification

| Source | Class | Type | Schema | Sensitivity | Ingestion Strategy |
|--------|-------|------|--------|-------------|-------------------|
| [Source 1] | [Small/Medium/Large] | [Type-2/Non-Type-2] | [Describe] | [PII/Confidential/Public] | [Full/Incremental/CDC] |

**Class definitions**:
- **Small**: <1GB - can use full refresh
- **Medium**: 1-100GB - should use incremental
- **Large**: >100GB - must use CDC/incremental

### Layer Requirements

#### Bronze Layer
- Data MUST land exactly as received (no transformations)
- Add metadata columns: `_ingested_at`, `_schema_version`
- Validation: distinct key counts, time ranges
- Use proper connectors (not generic JDBC for specialized sources)
- DBFS blob storage NOT for production data

#### Silver Layer
- Apply mappings and transformations
- Implement Type-2 SCD for change tracking
- Add historization columns: `start_date`, `end_date`, `is_current`
- Validate data before progressing to Gold

#### Gold Layer
- Customer-facing views only (no direct table access)
- Explicit column naming (no SELECT *)
- Map partition columns in predicates
- Apply optimization (Z-ordering, statistics)
- Configure proper ACLs

## Quality Gates *(lakehouse-specific)*

Define validation rules that MUST pass before data moves to Gold:

| Gate | Type | Rule | Enforcement |
|------|------|------|-------------|
| [Gate 1] | [Range/Cross-field/Statistical/Temporal] | [Rule definition] | [Block/Warn/Log] |

**Gate Types**:
- **Range validation**: Values within expected bounds
- **Cross-field validation**: Relationships between fields hold
- **Statistical validation**: Distributions match expectations
- **Temporal validation**: Time-based consistency checks

**Failed records handling**:
- [ ] Route to quarantine table for review
- [ ] Reject and alert
- [ ] Log and continue (only for non-critical)

## Requirements *(mandatory)*

<!--
  ACTION REQUIRED: The content in this section represents placeholders.
  Fill them out with the right functional requirements.
-->

### Functional Requirements

- **FR-001**: System MUST [specific capability, e.g., "land source data to Bronze without transformation"]
- **FR-002**: System MUST [specific capability, e.g., "implement CDC in Silver layer"]
- **FR-003**: Users MUST be able to [key interaction, e.g., "access Gold data via views only"]
- **FR-004**: System MUST [data requirement, e.g., "historize all dimension changes"]
- **FR-005**: System MUST [behavior, e.g., "validate data before Gold promotion"]

*Example of marking unclear requirements:*

- **FR-006**: System MUST ingest from [NEEDS CLARIFICATION: source system not specified]
- **FR-007**: System MUST retain history for [NEEDS CLARIFICATION: retention period not specified]

### Key Entities *(include if feature involves data)*

- **[Entity 1]**: [What it represents, key attributes without implementation]
- **[Entity 2]**: [What it represents, relationships to other entities]

## Success Criteria *(mandatory)*

<!--
  ACTION REQUIRED: Define measurable success criteria.
  These must be technology-agnostic and measurable.
-->

### Measurable Outcomes

- **SC-001**: [Measurable metric, e.g., "Bronze landing completes within SLA window"]
- **SC-002**: [Measurable metric, e.g., "Silver transformation processes >1M rows/hour"]
- **SC-003**: [Quality metric, e.g., "Quality gates catch >99% of data issues before Gold"]
- **SC-004**: [Business metric, e.g., "Gold views available within 15 minutes of source update"]
