# DevMirror — LH deployment notes

Standalone bundle at `customers/lh/databricks.yml`. Deployed to the LH Azure
workspace `https://adb-7405609894611143.3.azuredatabricks.net` using profile
`lh-dev`.

## Prerequisites (one-time per workspace)

- Account-level group `lhg-odp-adw-support-admin` exists and contains the
  LH operators who get admin role in the app.
- Catalog `odp_adw_support_n` and schema `odp_adw_support_n.devmirror` exist.
- SQL Warehouse `bda66ae121230af9` is reachable; the app SP and the
  deployer have `CAN_USE` on it.
- "Serverless compute for jobs" is enabled at the workspace level
  (Settings -> Compute). The lifecycle bundle uses serverless exclusively.
- Databricks CLI on the deployer's machine: 0.240+ (or set
  `DATABRICKS_TF_EXEC_PATH=$(which terraform)` to work around the expired
  Hashicorp PGP key in older CLI versions).

## Bootstrap (idempotent)

Creates the app, waits for ACTIVE, grants the app SP UC access on the
control schema:

```bash
./scripts/bootstrap_env.sh --profile lh-dev --catalog odp_adw_support_n --schema devmirror --warehouse-id bda66ae121230af9
```

`--warehouse-id` is what enables the new step 5 (control-table DDL).
If omitted, the script auto-discovers a RUNNING SQL warehouse and
fails if none are running.

If the LH bundle was deployed first (i.e. the app already exists from a
prior `bundle deploy`), bind the existing app to the bundle before
re-deploying:

```bash
export DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform
cd customers/lh && databricks bundle deployment bind devmirror devmirror --auto-approve --profile lh-dev
```

## Deploy

```bash
cd /Users/jince.james/projects/devmirror && ./scripts/build_app.sh
```

```bash
cd customers/lh && databricks bundle deploy --profile lh-dev && databricks bundle run devmirror --profile lh-dev
```

## REQUIRED post-deploy step — stream-search visibility

The DevMirror UI's "search streams" feature lists Databricks Jobs and DLT
pipelines via the Workspace SDK. Results are filtered server-side by the
caller's permissions. The app's auto-provisioned SP
(`app-5hh30h devmirror`, application_id `88ace2c4-a5cc-4583-b303-592549cd67f2`)
does NOT inherit any per-resource grants from the workspace-level
entitlements — it needs `CAN_VIEW` on each job and pipeline that should
appear in stream-search results.

**Required action after every deploy:** add the app SP (numeric SCIM id
`147783910414063`) as a member of an existing LH operator group that
already holds `CAN_VIEW`/`CAN_MANAGE` on the relevant jobs and pipelines.
This is future-proof — when new jobs/pipelines are granted to the group,
the SP picks them up automatically.

```bash
GROUP_ID=$(databricks groups list --profile lh-dev --filter "displayName eq 'lhg-odp-adw-support-admin'" --output json | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
```

```bash
databricks groups patch "$GROUP_ID" --profile lh-dev --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"members","value":[{"value":"147783910414063"}]}]}'
```

If `lhg-odp-adw-support-admin` doesn't already have CAN_VIEW on the
relevant resources, swap the group name above for one that does, or grant
CAN_VIEW to the group on each job/pipeline. Avoid adding the SP to the
workspace `admins` group — that grants CAN_MANAGE on everything and is
over-broad.

### Verify stream search

```bash
TOKEN=$(databricks auth token --profile lh-dev | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
curl -s -H "Authorization: Bearer $TOKEN" \
  "https://devmirror-7405609894611143.3.azure.databricksapps.com/api/streams/search?q=<job-or-pipeline-name-substring>" \
  | python3 -m json.tool
```

Should return an array of `{name, type}` results. If empty even when jobs
exist, the SP doesn't have visibility yet.

## Manual fixes still required (not yet automated)

This list captures every workaround the operator must remember during an
LH deploy. None of these are handled by `bootstrap_env.sh`, `build_app.sh`,
the bundle, or the lifecycle job. Each entry includes the "why" so the
TODO that retires the workaround is unambiguous.

### Once per workspace

1. **Bundle bind when the app already exists outside the bundle.**
   `bootstrap_env.sh` calls `databricks apps create` to provision the app
   and grant the auto-SP UC access. The first subsequent `bundle deploy`
   then fails because DAB sees an unmanaged app of the same name. Run
   bind once to adopt it:

   ```bash
   export DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform
   cd customers/lh && databricks bundle deployment bind devmirror devmirror --auto-approve --profile lh-dev
   ```

   *Why not automated:* `bootstrap_env.sh` predates the bundle-managed app
   model. **TODO:** drop the `apps create` step from `bootstrap_env.sh`
   when invoked for a customer bundle, OR have it call `bundle deployment
   bind` automatically after `apps create`.

2. **Grant `CAN_USE` on the SQL warehouse to the app SP.**
   `bootstrap_env.sh` accepts `--warehouse-id` (used for the new DDL
   step 5) but does NOT yet grant `CAN_USE` on it to the auto-SP.
   Any code path that falls back to the Statement Execution API
   (rare after the `_get_spark` fix, but not impossible) hits a
   permission error. One-liner:

   ```bash
   APP_SP=$(databricks apps get devmirror --profile lh-dev --output json | python3 -c "import sys,json;print(json.load(sys.stdin)['service_principal_client_id'])")
   databricks warehouses update-permissions bda66ae121230af9 --profile lh-dev --json "{\"access_control_list\":[{\"service_principal_name\":\"$APP_SP\",\"permission_level\":\"CAN_USE\"}]}"
   ```

   *Why not automated:* the warehouse-grant path differs from the
   catalog/schema grant the script already does. **TODO:** extend
   step 3 of `bootstrap_env.sh` to also grant `CAN_USE` on
   `--warehouse-id` to the auto-SP -- the warehouse id is already in
   scope thanks to step 5.

3. **Add the app SP to a privileged group for stream-search visibility.**
   See the "REQUIRED post-deploy step" section above. The app SP
   (`88ace2c4-a5cc-4583-b303-592549cd67f2`, SCIM id `147783910414063`)
   needs to inherit `CAN_VIEW` on jobs and pipelines from a group, since
   workspace entitlements alone don't grant per-resource visibility.

   *Why not automated:* customer-specific group choice. **TODO:** extend
   `bootstrap_env.sh` with `--operator-group <name>` and have it perform
   the SCIM patch.

4. **Confirm "Serverless compute for jobs" is enabled at the workspace
   level** (Settings → Compute). The lifecycle bundle uses serverless
   exclusively. If disabled, `bundle deploy` succeeds but the first
   scheduled run fails with a serverless-not-available error.

   *Why not automated:* this is a workspace admin toggle, not an API.
   No automation possible — keep as a documented prerequisite.

### Control-plane DDL gap (TABLE_OR_VIEW_NOT_FOUND on first run)

**Resolved as of 2026-05-06:** `bootstrap_env.sh` now applies the
control-table DDL automatically when invoked with `--warehouse-id`
(or auto-discovered RUNNING warehouse). Pass the warehouse id to your
LH bootstrap call -- see the updated invocation in the
"Bootstrap (idempotent)" section earlier in this README.

(Historical context, kept for older deployments:)
`bootstrap_env.sh` did NOT create the DevMirror control tables -- it
only handled app creation and grants.  The FastAPI app's lazy
bootstrap historically only ensured `devmirror_configs`, leaving the
other control tables (`devmirror_development_requests`,
`devmirror_dr_objects`, `devmirror_dr_access`, `audit_log`,
`devmirror_id_counter`) to be created out-of-band by `apply_control_ddl()`.

**Symptom:** runtime errors like
`[TABLE_OR_VIEW_NOT_FOUND] The table or view 'odp_adw_support_n'.'devmirror'.'audit_log' cannot be found`
on the first endpoint that hits a non-`devmirror_configs` table.

**Mitigations (defense-in-depth, both deployed):**

1. `bootstrap_env.sh` applies the DDL on onboarding when invoked with
   `--warehouse-id` (recommended path).
2. `app/backend/helpers.py:_get_repo()` also calls `apply_control_ddl()`
   on the first request that touches `ConfigRepository`, idempotently.
   Acts as a safety net for operators who run an older bootstrap
   script or upgrade in place.

**Manual fix if you hit this on an older deployment** (run once
against the customer's workspace and warehouse):

```bash
TOKEN=$(databricks auth token --profile lh-dev | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
HOST=https://adb-7405609894611143.3.azuredatabricks.net
WAREHOUSE=bda66ae121230af9
for f in /Users/jince.james/projects/devmirror/devmirror/migrations/*.sql; do
  python3 - <<PY
import json, urllib.request
text = open("$f").read().replace("{control_catalog}", "odp_adw_support_n").replace("{control_schema}", "devmirror")
# split on terminating semicolons; skip comment-only and empty lines
stmts, current = [], []
for line in text.splitlines():
    s = line.strip()
    if not current and (s.startswith("--") or not s):
        continue
    if s.startswith("--"):
        continue
    current.append(line)
    if s.endswith(";"):
        stmt = "\n".join(current).strip().rstrip(";")
        if stmt:
            stmts.append(stmt)
        current = []
for stmt in stmts:
    body = json.dumps({"statement": stmt, "warehouse_id": "$WAREHOUSE", "wait_timeout": "30s"}).encode()
    req = urllib.request.Request("$HOST/api/2.0/sql/statements", data=body,
        headers={"Authorization": "Bearer $TOKEN", "Content-Type": "application/json"}, method="POST")
    resp = json.loads(urllib.request.urlopen(req).read())
    print(resp.get("status", {}).get("state"), stmt[:70].replace("\n", " "))
PY
done
```

### App SP SCIM-read gap (admin-role detection)

The app SP `app-5hh30h devmirror` (`88ace2c4-a5cc-4583-b303-592549cd67f2`) is
**not a workspace admin**. Databricks SCIM restricts `GET /Users/{id}` and
`GET /Groups/{id}` to workspace admins, and `GET /Users?attributes=groups`
silently strips the `groups` attribute for non-admin callers. Empirically
confirmed against the LH workspace via a temporary `/api/debug/scim`
endpoint: the app SP sees `{ groups: [] }` for any user, even when the
user has 8 groups in their record.

This breaks the original group-centric admin lookup AND the user-centric
projection workaround. Mitigation deployed in code: the role resolver now
runs **on-behalf-of-user** (OBO), using the caller's OAuth token from
`X-Forwarded-Access-Token` to call SCIM `/Me` as the user themselves
(self-introspection always works regardless of admin status). For OBO to
fire, **`user_api_scopes` must be declared in `app/app.yaml`** (it is,
post-fix). Additionally, the LH workspace must have OBO/`user_api_scopes`
enabled at the platform level.

**Manual fixes that resolve this gap if OBO is not available in the LH
workspace** (rank-ordered, cheapest first):

1. **Make the app SP a workspace admin.** One SCIM patch — adds the SP
   (`147783910414063`) to the built-in `admins` group. Restores SCIM read
   access. Trade-off: the SP gains `CAN_MANAGE` on every workspace
   resource, so this is over-permissioned for what's needed.

   ```bash
   ADMINS_ID=$(databricks groups list --profile lh-dev --filter "displayName eq 'admins'" --output json | python3 -c "import sys,json;print(json.load(sys.stdin)[0]['id'])")
   databricks groups patch "$ADMINS_ID" --profile lh-dev --json '{"schemas":["urn:ietf:params:scim:api:messages:2.0:PatchOp"],"Operations":[{"op":"add","path":"members","value":[{"value":"147783910414063"}]}]}'
   ```

2. **Maintain a hardcoded admin-emails list as an env var (deployed
   for LH today).** `DEVMIRROR_ADMIN_EMAILS` is a comma-separated list
   that the role resolver consults BEFORE the SCIM lookup -- any caller
   whose `X-Forwarded-Email` matches (case-insensitive) is granted
   admin without needing SCIM. The SCIM-based group lookup still runs
   for callers not in the list, so the two paths layer naturally. Set
   the bundle var `--var admin_emails="alice@x.com,bob@y.com"` (or edit
   the default in `customers/lh/databricks.yml`). Trade-off: adding or
   removing an admin requires editing the var and redeploying. No
   workspace-admin grant needed; no SCIM permission needed.

3. **Have an LH workspace admin grant the app SP a SCIM read entitlement
   only** (not full admin). Databricks doesn't expose a fine-grained
   "SCIM read" permission today — this option is currently unsupported by
   the platform but worth filing as a feature request with Databricks if
   the customer wants stricter scope.

### Multi-workspace stream search (optional)

By default `/api/streams/search` only returns Jobs and DLT pipelines from
the workspace the app is deployed in.  To also search a second workspace
(e.g. an LH "prod" workspace, while DevMirror runs in "support"),
configure four bundle vars and an Apps Secret resource carrying a PAT
that has `CAN_VIEW` on the relevant resources in the remote workspace.

1. **Generate a PAT in the remote workspace** for a service principal
   that has CAN_VIEW on the jobs/pipelines you want surfaced.
2. **Store the PAT in a Databricks Secret** (in *this* (LH) workspace's
   secret scope, since the app reads from its own workspace):

   ```bash
   databricks secrets put-secret lh-devmirror remote-prod-pat --string-value "<the PAT>" --profile lh-dev
   ```

3. **Register an Apps Secret resource** so the app's auto-SP can read
   the secret value as an env var:

   ```bash
   databricks apps update devmirror --profile lh-dev --json '{"resources":[{"name":"remote-prod-pat","secret":{"scope":"lh-devmirror","key":"remote-prod-pat","permission":"READ"}}]}'
   ```

4. **Edit `app/app.yaml`** -- swap the
   `DEVMIRROR_REMOTE_WORKSPACE_TOKEN` env entry from `value: ""` to
   `valueFrom: remote-prod-pat`.

5. **Deploy with the remote-host vars set**:

   ```bash
   cd /Users/jince.james/projects/devmirror/customers/lh
   databricks bundle deploy --profile lh-dev \
     --var remote_workspace_host=https://<remote>.cloud.databricks.com \
     --var remote_workspace_label=prod \
     --var remote_workspace_token_secret=remote-prod-pat
   databricks bundle run devmirror --profile lh-dev
   ```

6. **Verify** by hitting `/api/streams/search?q=<job-name-fragment>` and
   confirming results from both workspaces appear, each with a
   `workspace` field set to the configured label. The UI shows each
   match with a coloured badge identifying its source workspace.

**Hard limit (out of scope until cross-workspace clone lands):** the
chosen stream must also exist in the *local* workspace's
`system.access.table_lineage` for the rest of the DevMirror flow (scan
-> clone -> grants) to succeed. If a user picks a stream that only
lives remotely, the scan step will return zero objects. Document this
when handing the feature to LH operators. **TODO:** track as future
"Stage 6 -- cross-workspace clone": carry `workspace_id` on `StreamRef`
and run lineage queries per-stream with the matching workspace's
client.

### Per deploy

5. **Set `DATABRICKS_TF_EXEC_PATH` in every shell that runs bundle
   commands.** The Databricks CLI 0.239.0 ships with an embedded Terraform
   downloader whose Hashicorp PGP signing key has expired. Every `bundle
   validate`, `bundle deploy`, `bundle run`, and `bundle deployment bind`
   fails with `unable to verify checksums signature: openpgp: key
   expired` until you point the CLI at a locally-installed Terraform:

   ```bash
   export DATABRICKS_TF_EXEC_PATH=/opt/homebrew/bin/terraform
   ```

   Persist it in `~/.zshrc` if you don't want to remember each shell.

   *Why not automated:* CLI bug, fixed upstream in 0.240+. **TODO:** bump
   the supported CLI floor in this README to 0.240+ and remove this
   workaround once all operators have upgraded.

6. **Synchronise `app/app.yaml`'s env block with the active target's
   values BEFORE `bundle run`.** Databricks Apps reads `app.yaml` from the
   uploaded source as the runtime config. The bundle's
   `apps.<name>.config.env` does NOT propagate to the running container —
   `databricks apps get` returns an empty `config.env` after deploy if
   the only source of env was the bundle. Today `app/app.yaml` is
   pinned to LH values; engineers deploying to AWS dev/prod must hand-
   edit the env block back to AWS values, then revert before any LH
   redeploy.

   *Why not automated:* `build_app.sh` doesn't know which target is
   active. **TODO:** add `build_app.sh --target lh|dev|prod` that writes
   `app/app.yaml`'s env block from the bundle target's variables. Until
   then, treat `app/app.yaml` as a per-customer artifact and never
   commit AWS-specific values to main.

7. **Re-run the stream-search SCIM grant if you've added new jobs or
   pipelines that should appear in search.** If you used Option A
   (per-resource bulk grant) instead of Option B (group membership) in
   the post-deploy step, new resources won't be visible until you
   re-run the grant loop. Group membership (Option B) avoids this
   recurring step.

## Documenting future gaps

When you hit a new gap during a customer deployment, capture it in the
"Manual fixes still required" section above following the same shape:

- **What broke** (the user-visible symptom)
- **Why it broke** (root cause, with file paths or API endpoints)
- **Manual fix(es) that resolve it** — concrete CLI commands, ranked
  cheapest first, with trade-offs called out
- **TODO** for retiring the workaround in code or scripts

Common gap categories worth scanning for on every new customer:
- Missing **workspace admin role** on the app SP (this README's biggest
  issue today — covered above)
- Missing **per-resource grants** the bootstrap script doesn't apply
  (warehouse `CAN_USE`, source-schema `SELECT`/`USE_SCHEMA`, target-
  catalog `CREATE_SCHEMA`)
- Missing **account-level group assignments** to the workspace (account
  groups must be assigned for SCIM to surface them)
- Missing **workspace-level toggles** the platform requires but the API
  doesn't expose (e.g. "Serverless compute for jobs" enabled, OBO
  `user_api_scopes` enabled)
- Missing **default ACLs** that customers' production workspaces lock
  down (jobs/pipelines visibility for the app SP)
- **CLI/tooling bugs** the operator must work around (the Databricks
  CLI 0.239 expired-PGP issue)

## Architectural decisions captured in the bundle (not manual fixes)

These were discovered during deployment and are now permanent properties
of `customers/lh/databricks.yml`. They do NOT need to be re-applied per
deploy:

- **No `run_as` block.** Databricks Apps require the app owner to match
  the deploying identity. With a customer-supplied `run_as` SP that
  differs from the deployer, `bundle validate` fails. Resolution: the
  lifecycle job and the app share the deployer's identity.
- **`workspace.root_path: /Workspace/Shared/devmirror/.bundle`.**
  Production mode requires an explicit root_path. Pinning to `/Shared`
  decouples artifact ownership from the deployer SP.
- **Serverless `compute.Environment` accepts only `client` +
  `dependencies`.** Env vars cannot be injected via the environment
  spec. Resolution: lifecycle tasks pass per-customer config as
  `python_wheel_task.named_parameters`, which `devmirror.jobs._apply_overrides_from_argv()`
  promotes to `DEVMIRROR_*` env vars before `load_settings()` runs.
- **Wheel build inside the bundle dir.** DAB sync rejects paths outside
  the bundle root, so the wheel is built into `customers/lh/dist/` and
  referenced as `dist/*.whl`.
- **`sync.paths: [../../app]`** to pull in the app source from the repo
  root without widening sync to the entire repo.
- **Lineage system table redirected to a curated view.** The LH
  workspace doesn't grant direct access to `system.access.table_lineage`;
  instead it exposes views under `odp_global.domain_adw` named
  `sys_<schema>_<table>` (e.g. `sys_access_table_lineage`). The bundle
  variable `lineage_system_table` (default
  `odp_global.domain_adw.sys_access_table_lineage`) feeds the env var
  `DEVMIRROR_LINEAGE_SYSTEM_TABLE`, which `Settings.lineage_system_table`
  consumes and `query_lineage()` uses verbatim. Other customer bundles
  can leave the var unset to fall back to the engine default
  `system.access.table_lineage`. The view's schema must match the system
  table's columns (`source_table_full_name`, `target_table_full_name`,
  `event_time`, etc. -- see `devmirror/scan/lineage.py:69-98`); LH
  confirmed this when the view was created.
