# DevMirror App: User Stories -- Stage 5 (Notifications)

---

## US-40: Receive a real email warning before my DR expires

**As an** owner of a Development Request approaching its expiration date,
**I want to** receive a real email warning,
**So that** I can extend the DR or back up data before the cleanup job removes
my schemas.

**Acceptance Criteria:**
1. When `DEVMIRROR_SMTP_HOST` is set, the lifecycle job sends an email per
   expiring DR via SMTP, using the existing `notify_expiring_drs` flow.
2. Recipients include the resolved members of any account-group entry in
   `lifecycle.notification_recipients` (or the fallback developers list) --
   never the raw group name.
3. Email subject and body match the existing `build_notification` content
   (reused unchanged).
4. SMTP failures are logged and recorded in the audit row (`status="FAILED"`,
   `error_message=...`); the lifecycle job continues with the next DR.
5. When `DEVMIRROR_SMTP_HOST` is unset, behavior falls back to `LoggingBackend`
   exactly as today (no regression).

**Priority:** P0

---

## US-41: Get Teams channel pings for visibility

**As an** operations team,
**I want to** receive a post in our Teams channel when a DR is expiring or a
new edit lands in the approval queue,
**So that** the team has shared visibility without each member getting per-user
email.

**Acceptance Criteria:**
1. When `DEVMIRROR_TEAMS_WEBHOOK_URL` is set (read from a Databricks secret
   reference at startup), Teams notifications are sent in addition to (not
   instead of) SMTP.
2. When unset, no Teams delivery is attempted and no error is raised.
3. The Teams payload is a simple Adaptive Card: title (subject), body (plain
   text), and a deep link back to the DR page (`{app_url}/config/<dr_id>` or
   `{app_url}/admin/approvals` for queue events).
4. Teams send failures do not block SMTP delivery and do not fail the
   lifecycle job -- they're logged at WARN.

**Priority:** P1

---

## US-42: Account groups in recipient lists fan out to actual users

**As an** admin,
**I want to** put account-group names in `notification_recipients` and
`developers` and have them fan out to actual user emails at delivery time,
**So that** every member of the group receives the notification even though
only the group name is in the config.

**Acceptance Criteria:**
1. At enqueue time, the notifier resolves each recipient: if it parses as an
   email, keep it; otherwise treat it as a group name and call
   `WorkspaceClient().groups.list(filter=...)` + `groups.get(id)` +
   `users.get(member.value)` to expand to member emails.
2. Resolution failures (group not found, member with no email) log a warning
   and are skipped silently -- the email still sends to the resolvable subset.
3. Resolution is cached per-process for 5 minutes (matching the role-cache TTL
   pattern in `auth.py`) so a job iterating 100 DRs that all reference the same
   group makes one SCIM lookup, not 100.

**Priority:** P0

---

## US-43: Get notified when a sensitive edit hits the approval queue

**As an** admin,
**I want to** receive an email (and optionally a Teams ping) when a developer's
sensitive edit lands in the approval queue,
**So that** I can act on it without polling the page.

**Acceptance Criteria:**
1. `app/backend/approvals.py::stage_pending_edit` calls the notifier's
   `enqueue()` once after writing the `CONFIG_EDIT_PENDING` audit row.
2. The notification's `kind` is `"approval_pending"`, recipients are members
   of `DEVMIRROR_ADMIN_GROUP` (resolved via SCIM), subject names the DR, body
   summarizes the changed fields and links to `/admin/approvals`.
3. If group resolution returns zero emails, the notifier still attempts a
   Teams ping (if configured); otherwise the call no-ops with a WARN log.
4. The same path is invoked when a `scanned` config completes auto-scan --
   admins get one notification telling them a config is ready to provision
   (covers the unification we already shipped at the queue-page level).

**Priority:** P1

---

## US-44: A stable enqueue() API so we can swap the delivery layer later

**As a** maintainer,
**I want** the notifier to expose a single
`enqueue(kind, recipients, subject, body, link)` entry point used by every
caller,
**So that** later swapping the SMTP/Teams direct-send for an outbox + SQL alert
becomes a one-file backend change.

**Acceptance Criteria:**
1. A new `Notifier` class in `devmirror/cleanup/notifier.py` exposes
   `enqueue(NotificationContent)` and (optionally) `flush()`.
2. The Notifier is constructed with a `list[NotificationBackend]` so SMTP +
   Teams can be active simultaneously.
3. Existing `notify_expiring_drs` is refactored to call `notifier.enqueue(...)`
   once per DR; the backend list is built once in `jobs.py::run_notifications`
   based on env vars.
4. Approval staging in `app/backend/approvals.py` uses the same Notifier
   (constructed at request time from env vars).
5. Each backend's `send()` method becomes an internal call invoked by
   `Notifier.enqueue` (synchronously for now). Switching to `OutboxBackend`
   later means: write a new backend that inserts to a Delta table and have a
   SQL alert deliver -- no caller change.

**Priority:** P0 (architectural)

---

## Non-functional requirements

- **Fail-safe.** Every backend wraps its delivery in try/except. Failures log
  + record but never raise into the caller.
- **No secret bleed.** SMTP credentials and the Teams webhook URL are read
  from env vars (which in production are populated by Databricks secrets via
  `valueFrom`); never logged, never in audit `action_detail`.
- **No per-call SCIM stampede.** The group-resolver 5-minute cache (US-42)
  covers this.
- **No new HTTP surface.** This stage only adds delivery; it does not change
  any HTTP endpoint.
- **No regression when SMTP is unconfigured.** Behavior falls back to today's
  `LoggingBackend` exactly.
