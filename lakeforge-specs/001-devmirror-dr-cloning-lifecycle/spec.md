# Feature Specification: DevMirror DR cloning lifecycle

**Feature Branch**: `001-devmirror-dr-cloning-lifecycle`  
**Created**: 2026-04-13  
**Status**: Draft  
**Input**: User description: "@SPECIFICATION.md (1-693); scope confirmed as full v1"

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Request isolated dev data from production streams (Priority: P1)

A platform operator or lead developer submits a development request that names one or more production data pipelines (streams), optional extra data objects, who needs access, how long the request lasts, and whether a separate QA replica space is required. The system discovers what production tables and views those streams depend on, produces a reviewable inventory with read/write semantics, and—after approval—provisions isolated developer and optional QA namespaces with replicas or governed references, plus access for the named people. The request is recorded as active with a clear status.

**Why this priority**: Without discovery, review, and first-time provisioning, no developer can safely work against production-shaped data in isolation.

**Independent Test**: Submit a minimal request for a known stream, complete review, run provisioning, and confirm an assigned developer can query assigned replica objects while unrelated developers cannot access that request’s namespaces.

**Acceptance Scenarios**:

1. **Given** a valid request configuration naming streams that exist in production, **When** discovery runs, **Then** the system produces an object inventory grouped by schema, each line classified as read-only, read-write, or write-only relative to the stream, and flags when extra human review is recommended.
2. **Given** an inventory that a reviewer adjusts (add object, remove object, change replica strategy where policy allows), **When** the reviewer approves, **Then** provisioning creates the required isolated schemas and objects, records each source-to-target mapping, and leaves the request in an active state suitable for use.
3. **Given** a request that names a stream that cannot be resolved, **When** validation runs, **Then** the submission fails with an explicit list of unresolved stream names and no partial provisioning occurs.

---

### User Story 2 - Refresh replicas to latest or an approved point in time (Priority: P2)

An assigned developer refreshes data for an active request to match either the latest production state or an approved historical snapshot, without rebuilding the entire request from scratch.

**Why this priority**: Data drifts; developers must repeat tests against current or known historical states.

**Independent Test**: With an active request, trigger refresh for all objects and for a single object subset; verify replica contents align to the chosen revision policy and timestamps update in the control record.

**Acceptance Scenarios**:

1. **Given** an active, unexpired request, **When** refresh is requested for “latest”, **Then** read-only references and replica objects reflect the latest authorized production state per policy, and the request records when refresh completed.
2. **Given** an active request and a snapshot choice that is still within retention for every in-scope object, **When** refresh runs, **Then** replicas align to that snapshot and failures per object are recorded without silently succeeding.
3. **Given** a snapshot choice outside retention for at least one object, **When** refresh is validated, **Then** the operation fails up front with guidance on the earliest usable snapshot information available for that object.

---

### User Story 3 - Evolve an active request safely (Priority: P3)

A lead adjusts an active request: add or remove streams or objects, add or remove people, extend or shorten the end date, or add/remove whole schemas. The system applies only the delta, keeps an audit trail, and tolerates partial failures with explicit per-action outcomes.

**Why this priority**: Long-lived requests need change management without full teardown.

**Independent Test**: Apply each modification type in isolation on a test request and verify control metadata, access, and object set match expectations.

**Acceptance Scenarios**:

1. **Given** an active request, **When** new streams are added, **Then** discovery merges new dependencies into the inventory, provisions only net-new objects, and preserves existing replicas unless superseded by policy.
2. **Given** an active request, **When** users are added or removed, **Then** access to that request’s namespaces matches the new list without affecting unrelated requests.
3. **Given** an active request, **When** the end date is moved later within policy limits, **Then** notifications and cleanup schedules recompute from the new date.

---

### User Story 4 - Lifecycle: warnings, expiry, and cleanup (Priority: P4)

The system warns stakeholders before an end date, then automatically tears down replicas, derived pipelines tied to the request, and access grants when the request expires, leaving a final audit trail.

**Why this priority**: Prevents runaway storage cost, stale copies, and lingering privileged access.

**Independent Test**: Create a request with a near-term end date; confirm warning delivery, then expiry cleanup removes namespaces and access while retaining audit history per retention policy.

**Acceptance Scenarios**:

1. **Given** an active request approaching its end date by the configured warning window, **When** the daily lifecycle check runs, **Then** each configured recipient receives a single consolidated notice per request (unless policy allows repeats) and the system records that the notice was sent.
2. **Given** a request whose end date is today or earlier and status is active, **When** cleanup runs, **Then** cloned jobs or pipelines for that request are removed, grants are revoked, objects and prefixed schemas are removed in a safe order, and the request ends in a completed-cleaned state or a retryable cleanup-in-progress state if something fails mid-way.
3. **Given** a request in cleanup with a retriable failure, **When** the next scheduled cleanup runs, **Then** remaining teardown steps continue without duplicating successful drops.

---

### User Story 5 - Transparency, audit, and status (Priority: P5)

Operators and developers can see request status, last refresh, inventory, and a chronological history of provision, refresh, modify, notify, and cleanup actions attributed to a user or system.

**Why this priority**: Governance and incident response require traceability.

**Independent Test**: Perform a sequence of operations on a request and export or query status and history; each step appears once with success, partial success, or failure detail.

**Acceptance Scenarios**:

1. **Given** any state transition on a request, **When** the transition completes, **Then** an audit entry exists with action type, actor, timestamp, outcome, and structured detail sufficient to reconstruct what changed.
2. **Given** a request identifier, **When** a stakeholder checks status, **Then** they see current lifecycle state, expiration date, last refresh time, and counts of provisioned objects without needing direct access to production.

---

### Edge Cases

- Stream lineage is incomplete or missing for dynamic references: inventory marks review-required; provisioning is blocked until a reviewer approves or augments the list.
- Object provisioning fails for one of many objects: other objects continue where policy allows; failed lines are visible with reasons; overall request remains usable with warnings when policy permits.
- Schema prefix collision with another active request: validation surfaces the conflict and which request holds the prefix; resolution is explicit (no silent overwrite).
- Production object removed while a read-through reference still exists: runtime queries may fail; the system does not promise automatic healing—re-scan and refresh are the supported path.
- Cleanup partially fails: request stays in a retryable cleanup state; next scheduled pass continues; no silent “cleaned” status.
- Re-submitting the same request identifier: treated as an update to the same logical request per configuration rules, not a duplicate parallel environment unless product policy says otherwise.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The system MUST accept a structured request definition that includes a unique identifier, one or more production streams, optional additional object names, optional QA space enablement, data snapshot policy (latest, fixed version, or timestamp), assigned people, and a mandatory end date within configurable maximum duration.
- **FR-002**: The system MUST validate that named streams resolve to known production orchestration definitions before discovery or provisioning proceeds.
- **FR-003**: The system MUST discover upstream and downstream data objects associated with each stream using authorized platform lineage metadata, enrich with optional curated mappings where configured, and classify each object’s relationship to the stream as read-only, read-write, or write-only.
- **FR-004**: The system MUST produce a human-reviewable inventory before first provisioning, capturing additions, removals, and strategy overrides from reviewers, and MUST block first provisioning until approval is recorded when review is required.
- **FR-005**: The system MUST provision isolated namespaces per request using consistent naming rules for development and optional QA, and MUST create replicas or governed references according to approved strategies (read-through, full independent copy, shared-file metadata copy, or empty schema-compatible shell) including optional point-in-time behavior aligned to the snapshot policy.
- **FR-006**: The system MUST grant each assigned person access only to namespaces and objects belonging to that request’s development or QA space, and MUST never grant write access to authoritative production objects as part of this product’s access model.
- **FR-007**: The system MUST record the full submitted definition, per-object mapping from production to isolated targets, snapshot policy, provision timestamps, refresh timestamps, and lifecycle status in a durable control store.
- **FR-008**: The system MUST support refresh of in-scope objects to latest or an approved snapshot, with per-object success or failure reporting and control-store updates.
- **FR-009**: The system MUST support post-provisioning modifications (streams, objects, schemas, people, end date) with incremental apply, partial-success semantics where explicitly allowed, and audit entries per change batch.
- **FR-010**: The system MUST send pre-expiry notifications to configured recipients with request summary, end date, and scope size, respecting configurable lead time.
- **FR-011**: The system MUST automatically clean up expired requests by removing derived orchestration copies, revoking grants, dropping isolated objects and schemas in dependency-safe order, and recording outcomes; incomplete cleanup MUST remain retryable.
- **FR-012**: The system MUST expose current request status and history of provision, refresh, modification, notification, and cleanup actions suitable for operator review.

### Key Entities *(include if feature involves data)*

- **Development request (DR)**: The unit of work; holds identity, description, lifecycle dates, status, snapshot policy, and links to people, streams, and inventories.
- **Stream**: A production orchestration definition (batch workflow or declarative pipeline) used as a root for dependency discovery.
- **Object inventory line**: A single production object with type, estimated size, access classification, chosen replica strategy, and review flags.
- **Replica mapping**: Association from a production object to an isolated target name, environment (development or QA), and timestamps for provision and last refresh.
- **Access binding**: A person (or group principal where supported) granted scoped rights on an isolated namespace for a specific environment.
- **Audit event**: Immutable-oriented log of an action on a request with actor, time, outcome, and machine-readable detail for support and governance.
- **System policy**: Administrator-controlled limits and defaults (maximum request length, warning lead time, concurrency limits, retention for audit history, optional size thresholds influencing replica strategy defaults).

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: For a reviewed and approved first-time request, at least 95% of inventory lines complete provisioning on the first attempt under nominal platform health, with every line showing either success or a recorded failure reason within the same run.
- **SC-002**: 100% of active requests receive a pre-expiry notification at or before the configured lead time, unless the request is cancelled or cleaned up earlier.
- **SC-003**: For requests past end date, automated cleanup reaches a terminal “fully cleaned” or explicit “cleanup in progress awaiting retry” state within 24 hours under nominal platform health, with zero retained access grants tied to that request’s isolated namespaces once fully cleaned.
- **SC-004**: 100% of user-visible state changes (submit, approve, provision completion, refresh, modification batch, notification sent, cleanup start/end) have a corresponding audit event retrievable by request identifier.
- **SC-005**: In access verification tests, zero unauthorized principals can read or write another request’s isolated namespaces without a policy-explicit cross-reference (default remains deny across requests).

## Assumptions

- Production and isolated environments share one governed catalog plane so replica and permission semantics are consistent; cross-metastore cloning is out of scope for this version.
- A service identity with clearly bounded elevated privileges performs provisioning and cleanup; individual developers are not required to hold broad production write privileges.
- Human review before first provisioning remains mandatory when lineage gaps are detected; optional on later refreshes only where administrators enable that relaxation.
