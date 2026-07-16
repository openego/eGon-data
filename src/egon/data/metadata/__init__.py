"""Run-time metadata task: assemble, validate, and publish OEMetadata.

Runs last in the pipeline. For every OEMetadata dataset in the split-YAML
store (one per DAG task group, see ``dataset_metadata/dataset_ids.yaml``):

1. Assemble the dataset document from the version-controlled YAML files.
2. Inject runtime information (publication date, automated-run
   contributor).
3. Merge the physical DB schema of every table present in this database;
   undocumented columns are merged with a TODO description placeholder
   and logged as drift warnings -- structural truth is never dropped and
   drift never fails the run. Tables absent from this database are
   skipped with an info log, so partial (dev) runs stay valid.
4. Validate against the OEMetadata standard (hard error -- the TODO merge
   guarantees schema conformance even for drifted tables).
5. Publish per table: a JSON table comment in the database and one
   ``db_schema.db_table.json`` file under ``workdir/oemetadata/``.

The version-controlled YAML store in git is the authoritative metadata
store; the database and the exported files are snapshots of what this
run produced. (A dedicated JSONB metadata table replacing the full-JSON
comments is planned but deliberately deferred.)
"""

import datetime
import json
import logging

from omi.creation.assembler import assemble_metadata_dict
from omi.creation.builder import MetadataBuilder
from omi.creation.utils import discover_dataset_ids
from omi.inspection import inspect_db_table

from egon.data import db
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.metadata.settings import OEM_BASE_DIR, OEM_OUT_DIR

logger = logging.getLogger(__name__)


def _inject_runtime_metadata(builder: MetadataBuilder) -> None:
    """
    Inject pipeline-specific runtime information into the OEMetadata builder.

    Sets the publicationDate and appends a CI pipeline contributor entry
    with values that are only known at pipeline execution time.

    Parameters
    ----------
    builder : omi.creation.builder.MetadataBuilder
        Initialized MetadataBuilder containing the base YAML metadata.
    """
    today_iso = datetime.date.today().isoformat()
    builder.set_publication_date(today_iso)
    builder.append_contributor_dataset(
        {
            "title": "CI Pipeline",
            "organization": "egon-data",
            "date": today_iso,
            "comment": "Automated dataset generation run",
        }
    )


def _merge_database_schemas(
    builder: MetadataBuilder, engine, resources: list
) -> list:
    """
    Inspect physical DB tables and merge their schemas into the metadata.

    The database provides structural truth (types, nullability); the YAML
    files provide descriptive truth (descriptions, units). Undocumented
    columns are merged with a TODO description placeholder; all drift is
    logged as warnings and never fails the run. Tables that do not exist
    in this database are skipped with an info log.

    Parameters
    ----------
    builder : omi.creation.builder.MetadataBuilder
        The MetadataBuilder instance to modify.
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine connected to the target database.
    resources : list of dict
        Resource dicts from the base metadata with 'schema.table' names.

    Returns
    -------
    list of str
        Names of the resources whose tables exist in this database.
    """
    present = []
    for resource in resources:
        full_name = resource.get("name")
        if not full_name:
            continue

        try:
            schema_name, table_name = full_name.split(".")
        except ValueError:
            logger.error(
                f"Invalid resource name '{full_name}'." " Must be schema.table"
            )
            continue

        try:
            db_skeleton = inspect_db_table(engine, schema_name, table_name)
        except Exception:
            logger.info(
                f"[{full_name}] table not present in this database;"
                " skipping."
            )
            continue

        present.append(full_name)
        try:
            # strict=False: pipeline completes even with missing descriptions
            drift_report = builder.resource(
                full_name
            ).merge_and_diff_db_schema(db_skeleton, strict=False)

            if drift_report.get("missing_in_yaml"):
                logger.warning(
                    f"[{full_name}] Undocumented columns in DB (merged with"
                    f" TODO description):"
                    f" {drift_report['missing_in_yaml']}"
                )
            if drift_report.get("missing_in_db"):
                logger.warning(
                    f"[{full_name}] Columns in YAML missing from DB:"
                    f" {drift_report['missing_in_db']}"
                )
            if drift_report.get("type_mismatches"):
                logger.warning(
                    f"[{full_name}] Type mismatches:"
                    f" {drift_report['type_mismatches']}"
                )

        except Exception as e:
            logger.error(f"Could not process schema for {full_name}: {e}")
    return present


def _publish_per_table(final_md: dict, present: list) -> None:
    """
    Publish per-table metadata: DB comment plus a JSON file per table.

    Isolates each resource block before serializing so neither the table
    comment nor the exported file carries the entire dataset's metadata.
    Only tables present in this database are published.

    Parameters
    ----------
    final_md : dict
        Finalized, validated OEMetadata dictionary containing all resources.
    present : list of str
        Resource names whose tables exist in this database.
    """
    OEM_OUT_DIR.mkdir(parents=True, exist_ok=True)
    for resource in final_md.get("resources", []):
        full_name = resource.get("name")
        if not full_name or full_name not in present:
            continue

        schema_name, table_name = full_name.split(".")

        single_resource_md = dict(final_md)
        single_resource_md["resources"] = [resource]

        meta_json = json.dumps(single_resource_md, ensure_ascii=False)
        db.submit_comment(meta_json, schema_name, table_name)
        logger.info(f"Metadata comment for {full_name} stored.")

        out_path = OEM_OUT_DIR / f"{full_name}.json"
        out_path.write_text(
            json.dumps(single_resource_md, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        logger.info(f"Metadata JSON written to {out_path}.")


def upload_json_metadata():
    """
    Orchestrate assembly, validation, and publication of dataset metadata.

    Central entry point for metadata integration at the end of the
    pipeline. Iterates every OEMetadata dataset in the split-YAML store;
    for each, loads the version-controlled YAML descriptions, injects
    runtime metadata, merges the physical DB schemas of the tables present
    in this database (drift is merged with TODO placeholders and logged),
    validates, and publishes one comment and one JSON file per table.
    """
    logger.info("Starting OMI metadata assembly and DB inspection...")
    engine = db.engine()

    for dataset_id in discover_dataset_ids(OEM_BASE_DIR):
        logger.info(f"Processing OEMetadata dataset '{dataset_id}'...")
        base_metadata = assemble_metadata_dict(OEM_BASE_DIR, dataset_id)
        builder = MetadataBuilder(base_metadata)

        _inject_runtime_metadata(builder)
        present = _merge_database_schemas(
            builder, engine, base_metadata.get("resources", [])
        )
        if not present:
            logger.info(
                f"No tables of dataset '{dataset_id}' present in this"
                " database; nothing to publish."
            )
            continue

        # skip license_policy to mirror previous pipeline behavior
        final_md = builder.build(
            validate_policy="validate", license_policy="skip"
        )

        _publish_per_table(final_md, present)
    logger.info("Metadata integration completed successfully.")


class Json_Metadata(Dataset):
    """
    Airflow dataset wrapper for the final metadata integration task.

    Executes upload_json_metadata after all upstream dependencies complete.
    """

    #: This task publishes metadata for other datasets' tables; it neither
    #: consumes nor produces data tables of its own.
    sources = DatasetSources()
    targets = DatasetTargets()

    def __init__(self, dependencies):
        super().__init__(
            name="JsonMetadata",
            version="0.0.0",
            dependencies=dependencies,
            tasks={upload_json_metadata},
        )
