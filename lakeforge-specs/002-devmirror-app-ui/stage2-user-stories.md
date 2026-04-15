# DevMirror App UI: User Stories -- Stage 2

---

## US-12: Trigger object discovery scan from the app

**As a** developer with a saved DR config,
**I want to** click a "Scan" button to discover which production objects my streams depend on,
**So that** I can see exactly what will be cloned before committing to provisioning.

**Acceptance Criteria:**
1. Each valid config in the list has a "Scan" action button.
2. Clicking "Scan" calls the backend, which resolves streams, queries lineage, and classifies objects.
3. While scanning, a spinner shows "Scanning streams..." (typically 5-30 seconds).
4. On completion, the config status updates to "scanned" and the user is navigated to the scan results page.
5. If stream resolution fails, an error message shows which streams could not be found.
6. Re-scanning a previously scanned config replaces the old manifest.

**Priority:** P0

---

## US-13: Review scan results before provisioning

**As a** developer who just ran a scan,
**I want to** review the list of discovered objects in a readable table,
**So that** I can verify the scope is correct before cloning anything.

**Acceptance Criteria:**
1. The scan results page shows a table with columns: Object FQN, Type (table/view), Access Mode (READ_ONLY/READ_WRITE/WRITE_ONLY), Estimated Size, Clone Strategy.
2. A summary banner shows: total objects, total schemas, review_required flag.
3. If `review_required` is true, a yellow warning banner says "This scan may have incomplete lineage. Please review carefully."
4. Each object row has a "Remove" button to exclude it from the manifest.
5. The streams that were scanned are listed above the table.
6. A "Back to Config" link returns to the config form.

**Priority:** P0

---

## US-14: Approve manifest and trigger provisioning

**As a** developer satisfied with the scan results,
**I want to** click "Approve & Provision" to create the isolated dev copies,
**So that** I can start working with the cloned data.

**Acceptance Criteria:**
1. The scan results page has an "Approve & Provision" button.
2. Clicking it shows a confirmation: "This will create X schemas and clone Y objects. Proceed?"
3. After confirmation, provisioning starts in the background and the user sees a progress page.
4. The config status updates to "provisioned" once complete.
5. If provisioning was already done for this DR, the button shows "Re-provision" with a warning that existing objects will be replaced.

**Priority:** P0

---

## US-15: Track provisioning progress

**As a** developer who triggered provisioning,
**I want to** see live progress updates,
**So that** I know what's happening and how long to wait.

**Acceptance Criteria:**
1. The progress page shows: DR ID, status ("provisioning..."), and a progress indicator.
2. The page polls the backend every 3 seconds for status updates.
3. Progress messages show the current step (e.g., "Creating schemas...", "Cloning objects 3/5...", "Applying grants...").
4. On success, the page shows a green summary: objects created, grants applied, final status.
5. On partial failure, the page shows a yellow summary with failed objects listed.
6. On complete failure, the page shows a red summary with error details.
7. A "View DR Status" button appears on completion.

**Priority:** P0

---

## US-16: View provisioned DR status and objects

**As a** developer or team lead,
**I want to** see the full status of a provisioned DR including all cloned objects,
**So that** I can verify everything is set up correctly and track the DR lifecycle.

**Acceptance Criteria:**
1. The DR status page shows: DR ID, status badge (ACTIVE/EXPIRING_SOON/etc.), description, expiration date, created by, last refreshed.
2. An objects table shows: source FQN, target FQN (dev), status (PROVISIONED/FAILED/DROPPED), clone strategy.
3. An object breakdown summary shows counts per status (e.g., "5 PROVISIONED, 0 FAILED").
4. A recent audit log section shows the last 10 actions with timestamps.
5. A "Cleanup" action button is available (with confirmation dialog).

**Priority:** P1

---

## US-17: List all provisioned development requests

**As a** team lead,
**I want to** see all active DRs across the team,
**So that** I can monitor resource usage and expiration dates.

**Acceptance Criteria:**
1. A dedicated "Active DRs" page shows a table of all provisioned DRs from the control table.
2. Columns: DR ID, Status, Description, Expiration Date, Object Count, Created By.
3. Status badges are colored: green (ACTIVE), yellow (EXPIRING_SOON), red (FAILED), gray (CLEANED_UP).
4. Clicking a DR row navigates to its status page.
5. The page is accessible from the main navigation.

**Priority:** P1

---

## US-18: Manually trigger cleanup from the app

**As a** developer who no longer needs a DR,
**I want to** clean it up from the app without using the CLI,
**So that** dev resources are freed promptly.

**Acceptance Criteria:**
1. The DR status page has a "Cleanup" button (only for ACTIVE/EXPIRING_SOON/EXPIRED DRs).
2. Clicking it shows a confirmation dialog: "This will drop X objects, revoke Y grants, and remove Z schemas. This cannot be undone."
3. Cleanup runs and the page shows the result: objects dropped, schemas removed, grants revoked.
4. The DR status updates to CLEANED_UP.
5. The "Cleanup" button is disabled for already cleaned-up DRs.

**Priority:** P1

---

## US-19: Background cleanup of expired DRs

**As a** platform administrator,
**I want** expired DRs to be cleaned up automatically without manual intervention,
**So that** stale dev environments don't consume resources indefinitely.

**Acceptance Criteria:**
1. The app runs a background loop that checks for expired DRs every 6 hours.
2. For each expired DR, it runs `cleanup_dr()` automatically.
3. Cleanup results are logged and visible in the audit log.
4. Failed cleanups are retried on the next cycle (DRs stay in CLEANUP_IN_PROGRESS).
5. The background loop does not block the app -- it runs in a separate async task.

**Priority:** P1

---

## US-20: Remove objects from scan manifest during review

**As a** developer reviewing scan results,
**I want to** remove objects I don't need from the manifest,
**So that** I only provision what's actually required and save storage.

**Acceptance Criteria:**
1. Each object row in the scan results table has a "Remove" button.
2. Clicking it removes the object from the manifest (no confirmation needed -- it's pre-provision).
3. The total objects count and estimated size update immediately.
4. Removed objects can be restored by re-scanning.
5. The modified manifest is saved when the user navigates away or clicks "Save Changes".

**Priority:** P2

---

## US-21: See scan status on config list

**As a** developer managing multiple configs,
**I want to** see at a glance which configs have been scanned, reviewed, or provisioned,
**So that** I know where each DR is in the workflow.

**Acceptance Criteria:**
1. The config list page shows a "Workflow" column with icons/badges: Draft, Valid, Scanned, Provisioned.
2. Available actions change based on workflow state:
   - Draft/Valid: "Scan" button
   - Scanned: "Review" and "Re-scan" buttons
   - Provisioned: "Status" button
3. The workflow column is sortable/filterable.

**Priority:** P2

---

## US-22: Navigate between config, scan, and DR status

**As a** user,
**I want** clear navigation between the config form, scan results, and DR status pages,
**So that** I can move through the workflow without getting lost.

**Acceptance Criteria:**
1. The config form has a "Scan" button (visible when config is valid).
2. The scan results page has breadcrumbs: Home > DR-1042 > Scan Results.
3. The provision progress page links to the DR status page on completion.
4. The DR status page has a "View Config" link back to the config form.
5. The main navigation has two sections: "Configs" (config list) and "Active DRs" (provisioned DR list).

**Priority:** P2

---

## Non-functional requirements

- **Long operations**: Scan and provision must not block the UI. Show progress indicators for anything taking >2 seconds.
- **Error resilience**: If a background task fails mid-way, the UI shows what succeeded and what failed. Partial provisioning is visible in the DR status page.
- **Idempotency**: Re-scanning replaces the old manifest. Re-provisioning with the same config replaces existing objects (schema collision handled per existing logic).
- **No data loss on app restart**: All state lives in Delta tables. In-memory task tracker is ephemeral but the DR control table always reflects the true state.
