"""DevMirror CLI entrypoint."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING

import click

from devmirror import __version__

if TYPE_CHECKING:
    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.settings import Settings
    from devmirror.utils.db_client import DbClient


# ------------------------------------------------------------------
# Shared context builder used by most subcommands
# ------------------------------------------------------------------


@dataclass
class _CliContext:
    settings: Settings
    db_client: DbClient
    dr_repo: DRRepository
    obj_repo: DrObjectRepository
    access_repo: DrAccessRepository
    audit_repo: AuditRepository


def _cli_context() -> _CliContext:
    """Load settings, build clients and repositories."""
    from databricks.sdk import WorkspaceClient

    from devmirror.control.audit import AuditRepository
    from devmirror.control.control_table import (
        DrAccessRepository,
        DrObjectRepository,
        DRRepository,
    )
    from devmirror.settings import SettingsError, load_settings
    from devmirror.utils.db_client import DbClient

    try:
        settings = load_settings()
    except SettingsError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    client = WorkspaceClient()
    db_client = DbClient(client=client)
    fqn = settings.control_fqn_prefix

    return _CliContext(
        settings=settings,
        db_client=db_client,
        dr_repo=DRRepository(fqn),
        obj_repo=DrObjectRepository(fqn),
        access_repo=DrAccessRepository(fqn),
        audit_repo=AuditRepository(fqn),
    )


@click.group()
@click.version_option(version=__version__, prog_name="devmirror")
def main() -> None:
    """DevMirror: clone production Unity Catalog objects into isolated dev environments."""


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True), help="Path to development request YAML config.")
def validate(config_path: str) -> None:
    """Validate a development request configuration file."""
    from pathlib import Path

    from devmirror.config.schema import load_development_request

    try:
        dr = load_development_request(Path(config_path))
        click.echo(f"Configuration valid: {dr.development_request.dr_id}")
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True), help="Path to development request YAML config.")
@click.option("--output", "output_path", required=True, type=click.Path(), help="Path to write the scan manifest YAML.")
def scan(config_path: str, output_path: str) -> None:
    """Scan production streams and generate an object manifest for review."""
    from pathlib import Path

    from devmirror.config.schema import DevMirrorConfigError, load_development_request

    try:
        config = load_development_request(Path(config_path))
    except DevMirrorConfigError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    ctx = _cli_context()
    manifest = _run_scan(config.development_request, ctx)

    from devmirror.scan.manifest import write_manifest

    out = Path(output_path)
    write_manifest(manifest, out)

    total = manifest["scan_result"]["total_objects"]
    review = manifest["scan_result"]["review_required"]
    click.echo(f"Manifest written to {out} ({total} objects, review_required={review}).")


def _run_scan(dr, ctx: _CliContext) -> dict:
    """Run the scan pipeline and return the manifest dict."""
    from databricks.sdk import WorkspaceClient

    from devmirror.scan.dependency_classifier import classify_dependencies
    from devmirror.scan.lineage import (
        get_enrichment_table,
        query_enrichment,
        query_lineage,
        query_table_sizes,
    )
    from devmirror.scan.manifest import build_manifest
    from devmirror.scan.stream_resolver import StreamResolutionError, resolve_streams

    client = WorkspaceClient()
    stream_names = [s.name for s in dr.streams]

    try:
        resolved, unresolved = resolve_streams(client, stream_names)
    except StreamResolutionError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)

    if unresolved:
        click.echo(f"Error: unresolved streams: {unresolved}", err=True)
        sys.exit(1)

    click.echo(f"Resolved {len(resolved)} stream(s).")

    lineage_result = query_lineage(
        ctx.db_client, resolved, lineage_table=ctx.settings.lineage_system_table
    )
    all_edges = list(lineage_result.edges)

    enrichment_table = get_enrichment_table()
    if enrichment_table:
        stream_keys = [s.name for s in resolved] + [s.resource_id for s in resolved]
        enrichment_result = query_enrichment(ctx.db_client, stream_keys, enrichment_table)
        all_edges.extend(enrichment_result.edges)

    classification = classify_dependencies(
        all_edges, additional_objects=dr.additional_objects
    )

    table_fqns = [obj.fqn for obj in classification.objects if obj.object_type == "table"]
    table_sizes = query_table_sizes(ctx.db_client, table_fqns) if table_fqns else {}

    return build_manifest(
        dr_id=dr.dr_id,
        streams=resolved,
        classification=classification,
        lineage_row_limit_hit=lineage_result.row_limit_hit,
        table_sizes=table_sizes or None,
    )


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True), help="Path to development request YAML config.")
@click.option("--manifest", "manifest_path", type=click.Path(exists=True), default=None, help="Path to an approved scan manifest YAML.")
@click.option("--auto-approve", is_flag=True, default=False, help="Run scan internally and skip manifest file (auto-approve).")
def provision(config_path: str, manifest_path: str | None, auto_approve: bool) -> None:
    """Provision DEV/QA environment from an approved manifest or via auto-approve."""
    from pathlib import Path

    from devmirror.config.schema import DevMirrorConfigError, load_development_request
    from devmirror.provision.runner import provision_dr
    from devmirror.scan.manifest import read_manifest

    if not manifest_path and not auto_approve:
        raise click.ClickException("Either --manifest or --auto-approve is required.")
    if manifest_path and auto_approve:
        raise click.ClickException("--manifest and --auto-approve are mutually exclusive.")

    try:
        config = load_development_request(Path(config_path))
    except DevMirrorConfigError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(2)

    ctx = _cli_context()

    if manifest_path:
        manifest = read_manifest(Path(manifest_path))
    else:
        click.echo("Running scan with auto-approve...")
        manifest = _run_scan(config.development_request, ctx)

    click.echo(f"Provisioning DR {config.development_request.dr_id}...")
    prov_result = provision_dr(
        config, manifest,
        db_client=ctx.db_client,
        dr_repo=ctx.dr_repo,
        obj_repo=ctx.obj_repo,
        access_repo=ctx.access_repo,
        audit_repo=ctx.audit_repo,
        max_parallel=ctx.settings.max_parallel_clones,
        force_replace=auto_approve,
    )

    ok, fail = len(prov_result.objects_succeeded), len(prov_result.objects_failed)
    click.echo(f"Provision {prov_result.dr_id}: {prov_result.final_status} "
               f"({ok} ok, {fail} failed, {prov_result.grants_applied} grants)")
    for obj in prov_result.objects_failed:
        click.echo(f"  FAIL {obj.source_fqn}: {obj.error}")

    if prov_result.final_status == "FAILED":
        sys.exit(1)


@main.command()
@click.option("--config", "config_path", type=click.Path(exists=True), default=None, help="Path to a refresh configuration YAML.")
@click.option("--dr-id", "dr_id", type=str, default=None, help="DR identifier (e.g. DR-1042).")
@click.option("--mode", "mode", type=click.Choice(["full", "incremental", "selective"]), default="incremental", help="Refresh mode.")
@click.option("--revision", "revision", type=str, default=None, help="Revision: 'latest', 'version:<N>', or 'timestamp:<ISO>'.")
@click.option("--objects", "selected_objects", type=str, default=None, help="Comma-separated source FQNs for selective mode.")
def refresh(config_path: str | None, dr_id: str | None, mode: str, revision: str | None, selected_objects: str | None) -> None:
    """Refresh DEV/QA objects from production for an active DR."""
    from pathlib import Path

    from devmirror.config.schema import DataRevision
    from devmirror.refresh.refresh_engine import RefreshError, refresh_dr

    if config_path and not dr_id:
        from devmirror.config.schema import load_development_request
        config = load_development_request(Path(config_path))
        dr_id = config.development_request.dr_id

    if not dr_id:
        raise click.ClickException("Either --dr-id or --config is required.")

    data_revision: DataRevision | None = None
    if revision and revision != "latest":
        if revision.startswith("version:"):
            data_revision = DataRevision(mode="version", version=int(revision.split(":", 1)[1]))
        elif revision.startswith("timestamp:"):
            data_revision = DataRevision(mode="timestamp", timestamp=revision.split(":", 1)[1])
        else:
            raise click.ClickException(f"Invalid --revision format: {revision!r}. Use 'latest', 'version:<N>', or 'timestamp:<ISO>'.")

    selected_fqns: list[str] | None = None
    if mode == "selective":
        if not selected_objects:
            raise click.ClickException("--objects is required for selective mode.")
        selected_fqns = [s.strip() for s in selected_objects.split(",") if s.strip()]

    ctx = _cli_context()

    click.echo(f"Refreshing DR {dr_id} (mode={mode})...")
    try:
        result = refresh_dr(
            dr_id, mode,  # type: ignore[arg-type]
            db_client=ctx.db_client,
            dr_repo=ctx.dr_repo,
            obj_repo=ctx.obj_repo,
            audit_repo=ctx.audit_repo,
            data_revision=data_revision,
            selected_fqns=selected_fqns,
            max_parallel=ctx.settings.max_parallel_clones,
        )
    except RefreshError as exc:
        raise click.ClickException(str(exc)) from exc

    ok, fail = len(result.objects_succeeded), len(result.objects_failed)
    click.echo(f"Refresh {result.dr_id}: {result.audit_status} "
               f"(mode={result.mode}, {ok} ok, {fail} failed)")
    for obj in result.objects_failed:
        click.echo(f"  FAIL {obj.source_fqn}: {obj.error}")

    if result.audit_status == "FAILED":
        sys.exit(1)


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(exists=True), help="Path to modification YAML config.")
@click.option("--add-streams", "add_streams_csv", type=str, default=None, help="Comma-separated stream names to add.")
def modify(config_path: str, add_streams_csv: str | None) -> None:
    """Apply modifications to an active DR (add/remove objects, users, change expiration)."""
    from pathlib import Path

    import yaml
    from databricks.sdk import WorkspaceClient

    from devmirror.config.schema import DataRevision
    from devmirror.modify.modification_engine import ModificationError, modify_dr

    cfg_path = Path(config_path)
    with cfg_path.open() as f:
        raw = yaml.safe_load(f)

    if not raw or "development_request" not in raw:
        raise click.ClickException("Modification YAML must contain a 'development_request' key.")

    dr_section = raw["development_request"]
    dr_id = dr_section.get("dr_id")
    if not dr_id:
        raise click.ClickException("'dr_id' is required in the modification config.")

    add_streams: list[str] | None = None
    if add_streams_csv:
        add_streams = [s.strip() for s in add_streams_csv.split(",") if s.strip()]

    data_revision: DataRevision | None = None
    rev_section = dr_section.get("data_revision")
    if rev_section:
        data_revision = DataRevision(**rev_section)

    ctx = _cli_context()
    client = WorkspaceClient() if add_streams else None

    click.echo(f"Modifying DR {dr_id}...")
    try:
        result = modify_dr(
            dr_id,
            db_client=ctx.db_client,
            dr_repo=ctx.dr_repo,
            obj_repo=ctx.obj_repo,
            access_repo=ctx.access_repo,
            audit_repo=ctx.audit_repo,
            add_objects=dr_section.get("add_objects"),
            remove_objects=dr_section.get("remove_objects"),
            add_dev_users=dr_section.get("add_developers"),
            remove_dev_users=dr_section.get("remove_developers"),
            add_qa_users=dr_section.get("add_qa_users"),
            remove_qa_users=dr_section.get("remove_qa_users"),
            new_expiration_date=str(dr_section["expiration_date"]) if dr_section.get("expiration_date") else None,
            data_revision=data_revision,
            add_streams=add_streams,
            client=client,
        )
    except ModificationError as exc:
        raise click.ClickException(str(exc)) from exc

    click.echo(f"Modify {result.dr_id}: {result.audit_status} ({len(result.actions)} actions)")
    for action in result.actions:
        tag = "OK" if action.success else "FAIL"
        click.echo(f"  [{tag}] {action.action}: {action.detail or action.error or ''}")

    if result.audit_status == "FAILED":
        sys.exit(1)


@main.command()
@click.option("--dr-id", "dr_id", required=True, type=str, help="DR identifier (e.g. DR-1042).")
def cleanup(dr_id: str) -> None:
    """Manually clean up a specific DR (drop objects, revoke grants, remove schemas)."""
    from devmirror.cleanup.cleanup_engine import cleanup_dr

    ctx = _cli_context()

    click.echo(f"Cleaning up DR {dr_id}...")
    result = cleanup_dr(
        dr_id,
        db_client=ctx.db_client,
        dr_repo=ctx.dr_repo,
        obj_repo=ctx.obj_repo,
        access_repo=ctx.access_repo,
        audit_repo=ctx.audit_repo,
    )

    click.echo(f"Cleanup {result.dr_id}: {result.final_status} "
               f"(dropped={result.objects_dropped}, schemas={result.schemas_dropped}, "
               f"revokes={result.revokes_succeeded})")
    for fqn, err in result.objects_failed:
        click.echo(f"  FAIL drop {fqn}: {err}")

    if result.final_status != "CLEANED_UP":
        sys.exit(1)


@main.command()
@click.option("--dr-id", "dr_id", required=True, type=str, help="DR identifier (e.g. DR-1042).")
@click.option("--json", "as_json", is_flag=True, default=False, help="Output as JSON.")
def status(dr_id: str, as_json: bool) -> None:
    """Show the status of a development request."""
    import json

    ctx = _cli_context()

    dr_row = ctx.dr_repo.get(ctx.db_client, dr_id=dr_id)
    if dr_row is None:
        raise click.ClickException(f"DR {dr_id} not found.")

    objects = ctx.obj_repo.list_by_dr_id(ctx.db_client, dr_id=dr_id)
    recent_audits = ctx.audit_repo.list_by_dr_id(ctx.db_client, dr_id=dr_id, limit=5)

    status_counts: dict[str, int] = {}
    for obj in objects:
        s = obj.get("status", "UNKNOWN")
        status_counts[s] = status_counts.get(s, 0) + 1

    if as_json:
        payload = {
            "dr_id": dr_row.get("dr_id"),
            "status": dr_row.get("status"),
            "description": dr_row.get("description"),
            "expiration_date": str(dr_row.get("expiration_date", "")),
            "created_at": str(dr_row.get("created_at", "")),
            "last_refreshed_at": str(dr_row.get("last_refreshed_at", "")),
            "last_modified_at": str(dr_row.get("last_modified_at", "")),
            "notification_sent_at": str(dr_row.get("notification_sent_at", "")),
            "total_objects": len(objects),
            "object_status_counts": status_counts,
            "recent_audit_actions": [
                {"action": a.get("action"), "status": a.get("status"), "performed_at": str(a.get("performed_at", ""))}
                for a in recent_audits
            ],
        }
        click.echo(json.dumps(payload, indent=2))
    else:
        for label, val in [
            ("DR ID", dr_row.get("dr_id")),
            ("Status", dr_row.get("status")),
            ("Description", dr_row.get("description", "")),
            ("Expiration Date", dr_row.get("expiration_date", "")),
            ("Created At", dr_row.get("created_at", "")),
            ("Last Refreshed At", dr_row.get("last_refreshed_at", "")),
            ("Last Modified At", dr_row.get("last_modified_at", "")),
            ("Notification Sent", dr_row.get("notification_sent_at", "")),
            ("Total Objects", len(objects)),
        ]:
            click.echo(f"{label + ':':<20}{val}")
        if status_counts:
            click.echo("Object Breakdown:")
            for s_name, cnt in sorted(status_counts.items()):
                click.echo(f"  {s_name}: {cnt}")
        if recent_audits:
            click.echo("Recent Audit Entries:")
            for a in recent_audits:
                click.echo(f"  [{a.get('performed_at', '')}] {a.get('action', '')} - {a.get('status', '')}")


@main.command(name="list")
def list_drs() -> None:
    """List all active and pending development requests."""
    ctx = _cli_context()
    drs = ctx.dr_repo.list_active(ctx.db_client)

    if not drs:
        click.echo("No active development requests found.")
        return

    click.echo(f"{'DR ID':<15} {'Status':<22} {'Expiration':<14} {'Description'}")
    click.echo("-" * 75)
    for dr in drs:
        desc = dr.get("description", "") or ""
        if len(desc) > 30:
            desc = desc[:27] + "..."
        click.echo(f"{dr.get('dr_id', ''):<15} {dr.get('status', ''):<22} {dr.get('expiration_date', '')!s:<14} {desc}")
