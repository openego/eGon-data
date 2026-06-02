import datetime
import json
import logging

from omi.creation.assembler import assemble_metadata_dict
from omi.creation.builder import MetadataBuilder
from omi.inspection import inspect_db_table

from egon.data import db
from egon.data.datasets import Dataset
from egon.data.metadata.settings import OEM_BASE_DIR, OEM_DATASET_ID

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
) -> None:
    """
    Inspect physical DB tables and merge their schemas into the metadata.

    The database provides structural truth (types, nullability); the YAML
    files provide descriptive truth (descriptions, units). Schema drift is
    logged as warnings.

    Parameters
    ----------
    builder : omi.creation.builder.MetadataBuilder
        The MetadataBuilder instance to modify.
    engine : sqlalchemy.engine.Engine
        SQLAlchemy engine connected to the target database.
    resources : list of dict
        Resource dicts from the base metadata with 'schema.table' names.
    """
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

        logger.info(f"Inspecting DB for {schema_name}.{table_name}")
        try:
            db_skeleton = inspect_db_table(engine, schema_name, table_name)

            # strict=False: pipeline completes even with missing descriptions
            drift_report = builder.resource(
                full_name
            ).merge_and_diff_db_schema(db_skeleton, strict=False)

            if drift_report.get("missing_in_yaml"):
                logger.warning(
                    f"[{full_name}] Undocumented columns in DB:"
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


def _publish_to_postgres(final_md: dict) -> None:
    """
    Attach per-table metadata as PostgreSQL table comments.

    Isolates each resource block before serializing to avoid storing the
    entire dataset's metadata in every table comment.

    Parameters
    ----------
    final_md : dict
        Finalized, validated OEMetadata dictionary containing all resources.
    """
    for resource in final_md.get("resources", []):
        full_name = resource.get("name")
        if not full_name:
            continue

        schema_name, table_name = full_name.split(".")

        single_resource_md = dict(final_md)
        single_resource_md["resources"] = [resource]

        meta_json = json.dumps(single_resource_md, ensure_ascii=False)

        logger.info(
            f"Writing metadata comment for"
            f" {schema_name}.{table_name} to db."
        )
        db.submit_comment(meta_json, schema_name, table_name)
        logger.info(f"Metadata comment for {schema_name}.{table_name} stored.")


def upload_json_metadata():
    """
    Orchestrate assembly, validation, and publication of dataset metadata.

    Central entry point for metadata integration at the end of the pipeline:
    1. Loads static YAML descriptions from version-controlled files.
    2. Injects runtime metadata (execution date, CI contributor).
    3. Merges physical DB schemas, logging any schema drift.
    4. Validates against the OEMetadata standard.
    5. Attaches the resulting JSON to the respective DB tables.
    """
    logger.info("Starting OMI metadata assembly and DB inspection...")
    engine = db.engine()

    base_metadata = assemble_metadata_dict(OEM_BASE_DIR, OEM_DATASET_ID)
    builder = MetadataBuilder(base_metadata)

    _inject_runtime_metadata(builder)
    _merge_database_schemas(
        builder, engine, base_metadata.get("resources", [])
    )

    # skip license_policy to mirror previous pipeline behavior
    final_md = builder.build(validate_policy="validate", license_policy="skip")

    _publish_to_postgres(final_md)
    logger.info("Metadata integration completed successfully.")


class Json_Metadata(Dataset):
    """
    Airflow dataset wrapper for the final metadata integration task.

    Executes upload_json_metadata after all upstream dependencies complete.
    """

    def __init__(self, dependencies):
        super().__init__(
            name="JsonMetadata",
            version="0.0.0",
            dependencies=dependencies,
            tasks={upload_json_metadata},
        )
