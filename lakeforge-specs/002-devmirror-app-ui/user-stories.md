# DevMirror App UI: User Stories -- Stage 1

---

## US-1: Create a new Development Request via form

**As a** developer who needs isolated dev data,
**I want to** fill out a web form with my DR details instead of writing YAML,
**So that** I can submit a correctly formatted request without learning the config file structure.

**Acceptance Criteria:**
1. The form has sections for: Basic Info, Streams, Environments, Data Revision, Access, and Lifecycle.
2. DR ID is auto-suggested as `DR-<next_number>` but editable.
3. Submitting the form calls the backend validation and stores the config.
4. On success, I see a confirmation with the DR ID and a "Valid" badge.
5. On validation failure, I see inline errors next to the fields that failed.

**Priority:** P0

---

## US-2: Search and add production streams

**As a** developer filling out the DR form,
**I want to** search for available Databricks workflows and pipelines by name,
**So that** I don't have to look up exact stream names separately.

**Acceptance Criteria:**
1. The Streams field has a typeahead search that queries the workspace.
2. Results show the stream name and type (job or pipeline).
3. I can add multiple streams to the list.
4. I can remove a stream from the list by clicking an X.
5. At least one stream is required -- the form shows an error if empty.

**Priority:** P0

---

## US-3: Validate a configuration before saving

**As a** developer,
**I want to** see all validation errors at once before I save,
**So that** I can fix everything in one pass instead of discovering errors one by one.

**Acceptance Criteria:**
1. Clicking "Validate & Save" runs both schema validation (field types, required fields, patterns) and policy validation (expiration within 90 days, QA users present if QA enabled).
2. Errors appear inline next to the relevant form fields with red highlights.
3. A summary banner at the top shows: "3 errors found" or "Configuration valid".
4. The config is stored with status `invalid` if errors exist, `valid` if none.
5. I can save a draft without validation by clicking "Save Draft".

**Priority:** P0

---

## US-4: View all my saved configurations

**As a** developer,
**I want to** see a list of all DR configurations I've created,
**So that** I can find, edit, or reuse them.

**Acceptance Criteria:**
1. The home page shows a table with columns: DR ID, Description, Status (valid/invalid), Expiration Date, Created At.
2. Each row has actions: Edit, Delete, Export YAML.
3. Invalid configs show a red badge; valid configs show a green badge.
4. The list is sorted by creation date (newest first).
5. Empty state shows a message with a "Create New" button.

**Priority:** P0

---

## US-5: Edit an existing configuration

**As a** developer,
**I want to** edit a previously saved DR configuration,
**So that** I can fix errors or update details without starting over.

**Acceptance Criteria:**
1. Clicking "Edit" on a config row opens the form pre-filled with all saved values.
2. I can change any field and re-validate.
3. Saving updates the existing record (same DR ID), not creating a duplicate.
4. The `updated_at` timestamp is refreshed.
5. If the DR has already been provisioned (Stage 2), editing is blocked with a message directing me to use the `modify` workflow instead.

**Priority:** P1

---

## US-6: Export configuration as YAML

**As a** developer who sometimes uses the CLI,
**I want to** download my saved config as a YAML file,
**So that** I can use it with `devmirror provision --config` from the command line.

**Acceptance Criteria:**
1. Clicking "Export YAML" downloads a `.yaml` file matching the DevMirror config format.
2. The file is valid -- running `devmirror validate --config <file>` succeeds.
3. The filename is `DR-<id>.yaml` (e.g., `DR-1042.yaml`).

**Priority:** P1

---

## US-7: Delete a saved configuration

**As a** developer,
**I want to** delete a config I no longer need,
**So that** the list stays clean.

**Acceptance Criteria:**
1. Clicking "Delete" shows a confirmation dialog ("Delete DR-1042?").
2. Confirming removes the config from storage and the list.
3. If the DR has been provisioned, deletion is blocked with a message to use cleanup first.

**Priority:** P2

---

## US-8: Conditional form fields based on selections

**As a** developer filling out the form,
**I want** fields to show/hide based on my selections,
**So that** I'm not confused by irrelevant options.

**Acceptance Criteria:**
1. When Data Revision mode is "Latest", no version/timestamp fields are shown.
2. When mode is "Specific Version", a version number input appears.
3. When mode is "Specific Timestamp", a datetime picker appears.
4. When QA environment is toggled off, the QA Users field is hidden.
5. When QA is toggled on and QA Users is empty, a warning appears on validate.

**Priority:** P1

---

## US-9: Auto-populate notification recipients from developers

**As a** developer,
**I want** notification recipients to default to the developers list,
**So that** I don't have to type the same emails twice.

**Acceptance Criteria:**
1. When the notification recipients field is empty, it shows placeholder text: "Defaults to developers list".
2. If I add developers and leave recipients blank, validation passes (backend uses developers as default).
3. I can override recipients by entering values explicitly.

**Priority:** P2

---

## US-10: Re-validate a stale configuration

**As a** developer who saved a config days ago,
**I want to** re-validate it against current policy rules,
**So that** I know if the expiration date has become invalid (too close to today).

**Acceptance Criteria:**
1. The config detail view has a "Re-validate" button.
2. Clicking it runs the backend validation against today's date.
3. If the config is now invalid (e.g., expiration is now in the past), the status updates to `invalid` with the relevant errors.
4. If still valid, a "Still valid" confirmation is shown.

**Priority:** P2

---

## US-11: See who created a configuration

**As a** team lead,
**I want to** see who created each DR config,
**So that** I know who to ask about a request.

**Acceptance Criteria:**
1. The config list and detail views show the `created_by` email.
2. The identity comes from the Databricks App OAuth token (no manual input).

**Priority:** P2

---

## Non-functional requirements

- **Responsive**: Form works on standard desktop screens (1280px+). Mobile not required.
- **Performance**: Config list loads in <2s. Validation returns in <3s. Stream search returns in <1s.
- **Error messages**: All validation errors use plain language. No stack traces, Pydantic internals, or technical jargon shown to users.
- **Accessibility**: Form fields have labels, required indicators, and error associations for screen readers.
