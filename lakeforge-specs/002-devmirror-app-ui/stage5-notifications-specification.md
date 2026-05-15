# DevMirror App: Real Notification Delivery

## Specification v5.0 -- Stage 5

---

## 1. Overview

Stage 5 replaces DevMirror's logging-only notification stub with real delivery
over SMTP (mandatory) and an optional Microsoft Teams channel webhook. It also
introduces a stable `Notifier.enqueue()` API so a future swap to a Databricks
SQL-Alert + outbox-table architecture is a backend change, not a caller change.

### 1.1 Why now

- DR expiration warnings currently land only in stdout. Owners learn their
  schemas are gone after the cleanup job has dropped them.
- The admin approval queue (Stage 4) only updates when an admin pulls the
  `/admin/approvals` page; pending edits sit invisible otherwise.
- Account-group entries (added in Stage 3 for `developers` / `qa_users` /
  `notification_recipients`) flow cleanly to the grant API but never get
  expanded to user emails for delivery -- they would only have worked once a
  real backend was in place.

### 1.2 Architecture decision

Two designs were considered:

- **Architecture A (deferred)** -- write notifications to a
  `devmirror_notification_outbox` Delta table and let a Databricks SQL Alert
  subscribed to a workspace-level Notification Destination deliver them. Most
  "native", but adds 1-5 minute alert latency, requires per-environment ops
  setup, and makes per-recipient routing harder.
- **Architecture C (Stage 5)** -- direct delivery from app code: an
  `SmtpBackend` (mandatory when configured) plus an optional
  `TeamsWebhookBackend`. Falls back to the existing `LoggingBackend` when
  nothing is configured.

Stage 5 ships Architecture C. The notifier is restructured around an
`enqueue()` method consumed by callers, so a future Stage 6 can introduce
`OutboxBackend(NotificationBackend)` (writing to a Delta table for a SQL Alert
to consume) without touching `notify_expiring_drs`, `stage_pending_edit`, or
any other call site.

### 1.3 Channels

| Channel | Status | Recipient model |
|---------|--------|-----------------|
| **SMTP email** | Required | Per-user. Account-group entries are expanded to member emails via SCIM at enqueue time. |
| **MS Teams channel webhook** | Optional | Channel post (one per notification). No per-user Teams DMs. |
| **LoggingBackend** | Always present as fallback | stdout only. Used when no real backend is configured. |

---

## 2. Notifier API

### 2.1 New `Notifier` class

`devmirror/cleanup/notifier.py` gains:

```python
class Notifier:
    def __init__(self, backends: list[NotificationBackend]):
        self._backends = backends

    def enqueue(self, content: NotificationContent) -> NotifierResult:
        """Fan content out to every configured backend.

        Each backend's send() is invoked under try/except. Failures are
        logged and recorded but do not raise into the caller. The return
        value reports per-backend success/failure so the caller can write
        an audit row.
        """
```

Every existing call site moves to `notifier.enqueue(content)`.

### 2.2 Backend list construction

`devmirror/jobs.py::run_notifications` and the new approval-queue path each
construct the `Notifier` once at entry, looking at env vars:

```
backends = []
if os.environ.get("DEVMIRROR_SMTP_HOST"):
    backends.append(SmtpBackend())
if os.environ.get("DEVMIRROR_TEAMS_WEBHOOK_URL"):
    backends.append(TeamsWebhookBackend())
if not backends:
    backends.append(LoggingBackend())
notifier = Notifier(backends)
```

The `LoggingBackend` is the only backend in dev / when SMTP isn't configured,
preserving today's behavior (no regression).

### 2.3 NotificationContent additions

The existing dataclass gains two optional fields used by the new backends and
unified queue UX:

- `kind: str` -- e.g. `"expiry_warning"`, `"approval_pending"`,
  `"provision_pending"`. Drives subject prefix and Teams card style.
- `link: str | None` -- a deep link back to the relevant DevMirror page
  (`{app_url}/config/<dr_id>` or `{app_url}/admin/approvals`).

---

## 3. SMTP Backend

### 3.1 Configuration

| Env var | Required | Default | Notes |
|---------|----------|---------|-------|
| `DEVMIRROR_SMTP_HOST` | Yes (to enable backend) | unset | Hostname; if unset, backend is not registered. |
| `DEVMIRROR_SMTP_PORT` | No | `587` | Validated as integer 1-65535 at `Settings` load time. |
| `DEVMIRROR_SMTP_USERNAME` | No | unset | If set, AUTH LOGIN. |
| `DEVMIRROR_SMTP_PASSWORD` | No (paired with USERNAME) | unset | Should be supplied via `valueFrom: secret` in `app.yaml`. |
| `DEVMIRROR_SMTP_FROM` | Yes (to enable backend) | unset | Envelope sender + From: header. |
| `DEVMIRROR_SMTP_USE_TLS` | No | `true` | STARTTLS on connect when truthy. |

### 3.2 Implementation

- Stdlib `smtplib.SMTP` + `email.message.EmailMessage`. No third-party
  dependency added.
- `send()` builds the message with `Subject:`, `From:`, `To:` (joined list of
  resolved emails), plain-text body. Returns `True` on `sendmail()` success;
  catches all exceptions, logs at WARN, returns `False`.
- TLS path: `SMTP(host, port).starttls()` before `login()` when `USE_TLS` is
  truthy.

### 3.3 Failure handling

- Per-DR audit row (`AuditRepository.append`) records `status="FAILED"` and
  `error_message=<exception text>` when SmtpBackend returns `False`.
- The lifecycle job's `notification_sent_at` is only set when at least one
  backend reports success, so failed notifications roll into the next run.

---

## 4. Teams Webhook Backend (optional)

### 4.1 Configuration

| Env var | Required | Default | Notes |
|---------|----------|---------|-------|
| `DEVMIRROR_TEAMS_WEBHOOK_URL` | Yes (to enable backend) | unset | Incoming-webhook URL for the target channel. Should be a `valueFrom: secret`. |

When unset, the backend is not registered. No error, no log noise.

### 4.2 Payload

Posts an Adaptive Card via `urllib.request.urlopen` (avoids adding `requests`
as a dep). Card content:

- **Title** -- the notification subject.
- **Body** -- plain-text body, truncated to 4 KB to fit Teams payload limits.
- **Action** -- "Open in DevMirror" linking to `content.link`.

### 4.3 Failure handling

- Network/HTTP failures log at WARN, return `False`. Never raises.
- A failed Teams send does not block SMTP; both backends run independently.

---

## 5. Group-to-Email Expansion (SCIM)

### 5.1 New helper `devmirror/cleanup/group_resolver.py`

```python
def resolve_recipients(items: list[str]) -> list[str]:
    """Expand mixed user-emails / group-names into a flat list of emails.

    Entries containing '@' are treated as user emails and passed through.
    Other entries are looked up via:
        WorkspaceClient().groups.list(filter="displayName eq '<name>'")
        WorkspaceClient().groups.get(<id>)
        WorkspaceClient().users.get(<member.value>) for each member
    The result is the union of pass-through emails plus all resolved
    member emails, deduplicated.
    """
```

### 5.2 Caching

A module-level `dict[str, tuple[list[str], float]]` caches results per group
name with a 5-minute TTL, mirroring the role-cache pattern in
`app/backend/auth.py::_resolve_role`. Thread-safe via a `threading.Lock`.

A nightly job iterating 100 DRs that all reference `data-engineers` makes one
SCIM round-trip, not 100.

### 5.3 Failure handling

- Group not found -> log INFO, skip that entry, return whatever else
  resolved.
- `users.get()` failures -> log WARN, skip that member, continue.
- The notifier still attempts delivery to the resolvable subset; a
  partially-empty recipient list is not an error.

---

## 6. Approval-Queue Notifications

### 6.1 Trigger

`app/backend/approvals.py::stage_pending_edit`, after writing the
`CONFIG_EDIT_PENDING` audit row, builds a `NotificationContent`:

- `kind="approval_pending"`
- recipients = members of `DEVMIRROR_ADMIN_GROUP` resolved via SCIM
- subject = `"DevMirror: pending edit on <dr_id>"`
- body summarizes the changed fields (one bullet per changed field)
- `link` = `{app_url}/admin/approvals`

It then calls `notifier.enqueue(content)`. The notifier is constructed at
request time inside `stage_pending_edit` from env vars (cheap; the SCIM and
Teams clients are lazy).

### 6.2 Scan-completion notification

The same path fires when an auto-scan completes successfully and the config
moves to `status="scanned"` (covers the "approve & provision" leg of the
unified queue page from Architecture A's UI merge).

### 6.3 Empty-recipient fallback

If group resolution yields zero emails AND no Teams webhook is configured,
the call no-ops with a WARN log -- the audit log still has a record of the
pending edit, so the admin queue page surfaces the work even without a
notification.

---

## 7. Implementation Files

### 7.1 New

- `devmirror/cleanup/backends/__init__.py`
- `devmirror/cleanup/backends/smtp.py` -- `SmtpBackend`
- `devmirror/cleanup/backends/teams.py` -- `TeamsWebhookBackend`
- `devmirror/cleanup/group_resolver.py` -- `resolve_recipients` + cache

### 7.2 Modified

- `devmirror/cleanup/notifier.py` -- add `Notifier`, refactor
  `notify_expiring_drs` to consume it; keep `LoggingBackend` for fallback.
- `devmirror/jobs.py::run_notifications` -- build backend list from env;
  construct `Notifier`; pass to `notify_expiring_drs`.
- `app/backend/approvals.py::stage_pending_edit` -- enqueue notification
  after staging the audit row.
- `devmirror/settings.py` -- add SMTP and Teams env-var fields; validate
  SMTP port range when set.
- `app/app.yaml` -- commented placeholders for SMTP and Teams env vars,
  with `valueFrom: secret` references documented.

### 7.3 Tests

- `tests/unit/test_notifier.py` -- new `Notifier` fan-out tests, multi-backend
  pass/fail, audit-row content on failures.
- `tests/unit/test_smtp_backend.py` -- new; mocks `smtplib.SMTP`; covers TLS,
  auth, error paths.
- `tests/unit/test_teams_backend.py` -- new; mocks `urllib.request.urlopen`;
  asserts Adaptive-Card JSON shape; HTTP error path.
- `tests/unit/test_group_resolver.py` -- new; SCIM mocked; expansion +
  cache-hit assertions.
- `app/tests/test_admin_approvals.py` -- assert `stage_pending_edit` triggers
  the notifier with `kind="approval_pending"`.

---

## 8. Non-functional Requirements

- **Fail-safe.** Every backend's `send()` wraps everything in `try/except`
  and returns `True`/`False`. The notifier records per-backend results in an
  audit row but never propagates exceptions to the caller.
- **No secret leakage.** SMTP credentials and the Teams webhook URL are read
  from env vars (sourced from Databricks secrets via `valueFrom`); never
  logged, never appear in audit `action_detail`.
- **No per-call SCIM stampede.** The 5-minute group-resolver cache covers
  bulk runs.
- **No new HTTP surface.** No new endpoints; this stage is delivery only.

---

## 9. Out of Scope

- Architecture A: outbox table + SQL Alert + `OutboxBackend`. Future Stage 6.
- Per-user Teams direct messages (would need MS Graph + per-user OAuth).
- HTML email templates. Body remains the plain-text format produced by
  `build_notification`.
- Retries on transient SMTP failures. The next nightly run picks up unsent
  rows automatically (since `notification_sent_at` only sets on success).
- Notification preferences per user (mute, digest, etc.).

---

## 10. Risks & Open Questions

| Risk | Mitigation |
|---|---|
| Customer SMTP server requires non-standard auth (OAuth XOAUTH2, Kerberos) | Stage 5 ships PLAIN/LOGIN AUTH only. Customers needing OAuth fall back to `LoggingBackend` until a future enhancement. |
| Teams webhook URL leaks in logs / audit | URL is read once at backend construction; never logged in send paths. Audit `action_detail` records `kind` and recipient count only, not the URL. |
| Group resolution adds latency to approval staging | One SCIM round-trip per unique group, cached 5 min. Approval staging is a single click; latency budget is generous. |
| SCIM service principal lacks `groups:read` on the workspace | The role resolver in `auth.py` already needs this permission; stage 5 reuses the same SP. No new grant needed. |
