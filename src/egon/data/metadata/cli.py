"""Developer CLI for the metadata store: status, init, export.

The three phase-2 commands of the metadata workflow (one OEMetadata
dataset per DAG task group, split-YAML store under ``dataset_metadata/``):

- ``egon-data metadata status`` -- three-state coverage report
  (missing / skeleton / complete) of declared target tables against the
  YAML store, plus provenance warnings for consumed source tables.
- ``egon-data metadata init`` -- create or non-destructively update
  resource YAML skeletons: columns are inspected from the local database
  where available (graceful degradation without one), provenance stubs
  are derived from ``DatasetSources``.
- ``egon-data metadata export`` -- write one OEMetadata JSON per table
  (``db_schema.db_table.json``) assembled from the YAML store alone.

All commands accept ``--scope`` (a task group, an OEMetadata dataset id,
or a Dataset class name) so developers only see their own gaps. Without
``--scope`` the full store is processed -- the same code path CI uses.
"""

from __future__ import annotations

import json

import click

from egon.data.metadata.settings import (
    OEM_BASE_DIR,
    OEM_OUT_DIR,
    OEMETADATA_VERSION,
)


def _scoped_classes(scope):
    """Return declared-IO classes, filtered by --scope when given."""
    from egon.data.metadata.inventory import filter_scope, scan_declared_io

    classes = scan_declared_io()
    if scope:
        scoped = filter_scope(classes, scope)
        if not scoped:
            raise click.UsageError(
                f"--scope '{scope}' matches no task group, dataset id, or"
                " Dataset class."
            )
        return classes, scoped
    return classes, classes


def _dataset_ids_for(classes):
    """Map the classes' task groups to their OEMetadata dataset ids."""
    from egon.data.metadata.inventory import UNMAPPED, load_dataset_ids

    ids = load_dataset_ids()
    out = {}
    for c in classes:
        if c.task_group == UNMAPPED:
            continue
        ds_id = ids.get(c.task_group)
        if ds_id:
            out[c.task_group] = ds_id
    return out


def _try_engine():
    """Return a connected engine or None (graceful no-DB degradation)."""
    try:
        from egon.data import db

        engine = db.engine()
        with engine.connect():
            pass
        return engine
    except Exception as error:  # noqa: BLE001
        click.secho(
            f"No local database reachable ({error}); continuing without"
            " schema inspection.",
            fg="yellow",
        )
        return None


@click.group("metadata")
def metadata():
    """Manage OEMetadata for the pipeline's published tables."""


@metadata.command("status")
@click.option(
    "--scope",
    default=None,
    help="Task group, OEMetadata dataset id, or Dataset class name.",
)
@click.option(
    "--strict",
    is_flag=True,
    help="Exit non-zero if any expected table has no metadata (CI gate).",
)
def status_cmd(scope, strict):
    """Report metadata coverage: missing / skeleton / complete."""
    from omi.creation.coverage import coverage_report

    from egon.data.metadata.inventory import consumed_tables, expected_tables

    all_classes, scoped = _scoped_classes(scope)
    expected = expected_tables(scoped)
    ds_ids = _dataset_ids_for(scoped)

    report = coverage_report(
        OEM_BASE_DIR,
        sorted(expected),
        dataset_ids=sorted(set(ds_ids.values())) if scope else None,
    )
    click.echo(report.summary())
    for name in report.missing:
        producers = ", ".join(c.name for c in expected[name])
        click.secho(f"[missing]  {name}  (produced by {producers})", fg="red")
    for state in report.skeleton:
        detail = "; ".join(state.reasons[:3])
        more = len(state.reasons) - 3
        if more > 0:
            detail += f"; +{more} more"
        click.secho(f"[skeleton] {state.name}  ({detail})", fg="yellow")
    for name in report.orphans:
        click.secho(f"[orphan]   {name}", fg="cyan")

    # Provenance: a consumed internal table should have metadata of its
    # own, otherwise cross-references made by `init` point nowhere. Check
    # against the whole store -- the table may belong to another dataset.
    from omi.creation.coverage import index_store_resources

    produced_all = expected_tables(all_classes)
    documented = set(index_store_resources(OEM_BASE_DIR))
    for table, consumers in sorted(consumed_tables(scoped).items()):
        if table in produced_all and table not in documented:
            users = ", ".join(c.name for c in consumers)
            click.secho(
                f"[source]   {table} is consumed (by {users}) but has no"
                " metadata yet",
                fg="yellow",
            )

    if strict and not report.ok:
        raise click.exceptions.Exit(1)


@metadata.command("init")
@click.option(
    "--scope",
    default=None,
    help="Task group, OEMetadata dataset id, or Dataset class name.",
)
def init_cmd(scope):
    """Create or update resource YAML skeletons (non-destructive)."""
    from omi.creation.init import init_dataset

    _, scoped = _scoped_classes(scope)
    ds_ids = _dataset_ids_for(scoped)
    engine = _try_engine()

    for group, ds_id in sorted(ds_ids.items()):
        result = init_dataset(
            OEM_BASE_DIR, ds_id, oem_version=OEMETADATA_VERSION
        )
        click.echo(f"dataset {ds_id} ({group}): {result.dataset_yaml}")

    for c in sorted(scoped, key=lambda c: c.name):
        ds_id = ds_ids.get(c.task_group)
        if ds_id is None:
            click.secho(
                f"[skip] {c.name}: task group '{c.task_group}' has no"
                " dataset id",
                fg="yellow",
            )
            continue
        for table in sorted(c.target_tables.values()):
            _init_resource(c, ds_id, table, engine)


def _init_resource(c, ds_id, table, engine):
    """Create or update one resource YAML for a produced table."""
    from omi.creation.init import (
        add_resource_from_oem_metadata,
        update_resource_yaml_from_db,
    )
    from omi.creation.sources import (
        add_source_to_resource_file,
        build_external_source,
        build_internal_source,
    )
    from omi.inspection import InspectionError, inspect_db_table

    safe = table.replace(".", "_")
    path = OEM_BASE_DIR / "resources" / ds_id / f"{safe}.resource.yaml"

    created = not path.exists()
    if created:
        skeleton = {"name": table}
        if engine is not None and "." in table:
            schema_name, table_name = table.split(".", 1)
            try:
                skeleton = inspect_db_table(engine, schema_name, table_name)
            except InspectionError:
                click.secho(
                    f"[no schema] {table}: not present in local DB;"
                    " writing stub without fields",
                    fg="yellow",
                )
        add_resource_from_oem_metadata(
            OEM_BASE_DIR,
            ds_id,
            {"resources": [skeleton]},
            resource_name=safe,
            oem_version=OEMETADATA_VERSION,
            fill_missing_from_template=True,
        )
        click.secho(f"[created]  {path}", fg="green")
    elif engine is not None:
        try:
            _, report = update_resource_yaml_from_db(
                OEM_BASE_DIR, ds_id, table, engine
            )
            drift = {k: v for k, v in report.items() if v}
            if drift:
                click.secho(f"[updated]  {path}  drift={drift}", fg="yellow")
        except (FileNotFoundError, InspectionError) as error:
            click.secho(f"[no update] {table}: {error}", fg="yellow")

    # Provenance stubs from the producing class's declared sources -- only
    # into freshly created files: existing (possibly curated) YAMLs may
    # already describe the same sources under richer titles, which the
    # title+path de-duplication cannot recognize.
    if not created:
        return
    for src_table in sorted(c.source_tables.values()):
        add_source_to_resource_file(path, build_internal_source(src_table))
    for key, url in sorted(c.source_urls.items()):
        add_source_to_resource_file(path, build_external_source(key, path=url))
    for key, file_path in sorted(c.source_files.items()):
        add_source_to_resource_file(
            path, build_external_source(key, path=file_path)
        )


@metadata.command("export")
@click.option(
    "--scope",
    default=None,
    help="Task group, OEMetadata dataset id, or Dataset class name.",
)
def export_cmd(scope):
    """Write one OEMetadata JSON per table from the YAML store alone."""
    from omi.creation.assembler import assemble_metadata_dict

    if scope:
        _, scoped = _scoped_classes(scope)
        ds_ids = sorted(set(_dataset_ids_for(scoped).values()))
    else:
        from omi.creation.utils import discover_dataset_ids

        ds_ids = discover_dataset_ids(OEM_BASE_DIR)

    OEM_OUT_DIR.mkdir(parents=True, exist_ok=True)
    n = 0
    for ds_id in ds_ids:
        try:
            document = assemble_metadata_dict(OEM_BASE_DIR, ds_id)
        except FileNotFoundError:
            click.secho(f"[skip] dataset '{ds_id}' not in store", fg="yellow")
            continue
        for resource in document.get("resources", []):
            name = resource.get("name")
            if not name:
                continue
            single = dict(document)
            single["resources"] = [resource]
            out_path = OEM_OUT_DIR / f"{name}.json"
            out_path.write_text(
                json.dumps(single, indent=2, ensure_ascii=False),
                encoding="utf-8",
            )
            n += 1
    click.echo(f"{n} metadata JSON files written to {OEM_OUT_DIR}/")
