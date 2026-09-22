"""The central module containing all code dealing with importing ETHOS.BUILDA.

ETHOS.BUILDA is a synthetic building stock dataset for Germany. It supplies one
point per residential building together with construction year, size class,
refurbishment state and TABULA type. eGon-data uses it to classify OSM building
polygons as residential by spatial intersection instead of relying on OSM tags
alone, see
:py:mod:`osm_buildings_streets <egon.data.datasets.osm_buildings_streets>`.

The data is published per NUTS-1 region (federal state) as one CSV each, which
is also how the test mode is implemented: only Schleswig-Holstein (``DEF``) is
downloaded and imported.
"""

from pathlib import Path
from urllib.request import urlretrieve
import hashlib
import os

from sqlalchemy import text

from egon.data import db, logger
from egon.data.config import settings
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
import egon.data.subprocess as subprocess

#: Zenodo record of ETHOS.BUILDA v2.0.0, DOI 10.5281/zenodo.13771740
ZENODO_RECORD = "13771740"

#: Template for a file download from the Zenodo REST API
ZENODO_FILE_URL = (
    "https://zenodo.org/api/records/{record}/files/{filename}/content"
)

#: Local download directory, relative to the egon-data working directory
DOWNLOAD_DIRECTORY = Path(".") / "ethos_builda"

#: One CSV per NUTS-1 region, with size and MD5 checksum as published on
#: Zenodo. Size is used to detect an already complete download, the checksum
#: verifies a fresh one.
ETHOS_FILES = {
    "DE1": {"size": 840685377, "md5": "aab2d48de4a826c71004543333455327"},
    "DE2": {"size": 1275700666, "md5": "a37c61c533f74f4783b404c258eb055c"},
    "DE3": {"size": 103955160, "md5": "c0166995f74f38503ceb8b9542d78924"},
    "DE4": {"size": 288550765, "md5": "c8b3c4371ffa0d5e0c014af625778dba"},
    "DE5": {"size": 44734828, "md5": "e951342ad810f396415a3d4a18a05569"},
    "DE6": {"size": 88055813, "md5": "fa9c147ca0c5527a02cb66681466ab9d"},
    "DE7": {"size": 714990713, "md5": "210750cb89c0a061d8957385ebde8e0e"},
    "DE8": {"size": 95970367, "md5": "58ac122303efe1fac646ca4ebbe69c8f"},
    "DE9": {"size": 857126161, "md5": "5ae5a296d8a7fe16c3e2c84fc10a9938"},
    "DEA": {"size": 1601606107, "md5": "2696cf5189bc089231ee4fdb09943601"},
    "DEB": {"size": 360513970, "md5": "d6dba34ca3665befea6b80d15ac2121b"},
    "DEC": {"size": 103194206, "md5": "fd4c6344d16b5368d4974f0ff7708d05"},
    "DED": {"size": 323823386, "md5": "bfbfd45d792fd130a39ba816789e6fb6"},
    "DEE": {"size": 319897115, "md5": "2db4d12a20769be9991600df046d641b"},
    "DEF": {"size": 221670474, "md5": "721036d1ca2255164e3ab86604d7b4f9"},
    "DEG": {"size": 271610575, "md5": "0d6b6a8e848803318d2ea7168af53f4e"},
}

#: NUTS-1 region imported in test mode (Schleswig-Holstein)
TESTMODE_NUTS1 = "DEF"


def nuts1_regions():
    """NUTS-1 regions to import, depending on the dataset boundary.

    Returns
    -------
    list of str
        All 16 NUTS-1 codes for ``Everything``, only
        :py:data:`TESTMODE_NUTS1` otherwise.
    """
    if settings()["egon-data"]["--dataset-boundary"] == "Everything":
        return sorted(ETHOS_FILES)
    logger.info(f"Using testmode dataset, only {TESTMODE_NUTS1}.")
    return [TESTMODE_NUTS1]


def download():
    """Download the ETHOS.BUILDA CSV files from Zenodo.

    Files already present with the published size are kept. A freshly
    downloaded file is verified against its published MD5 checksum; an
    existing one is not re-hashed, since that would read several gigabytes on
    every pipeline run.
    """
    DOWNLOAD_DIRECTORY.mkdir(exist_ok=True)

    for nuts1 in nuts1_regions():
        filename = EthosBuilda.targets.files[nuts1]
        target_file = DOWNLOAD_DIRECTORY / filename
        published = ETHOS_FILES[nuts1]

        if (
            target_file.is_file()
            and target_file.stat().st_size == published["size"]
        ):
            logger.info(f"{filename} already downloaded, skipping.")
            continue

        logger.info(f"Downloading {filename} ({published['size']} bytes)...")
        urlretrieve(EthosBuilda.sources.urls[nuts1], target_file)

        digest = hashlib.md5()
        with open(target_file, "rb") as f:
            for chunk in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(chunk)
        if digest.hexdigest() != published["md5"]:
            raise ValueError(
                f"MD5 mismatch for {filename}: expected {published['md5']}, "
                f"got {digest.hexdigest()}."
            )
        logger.info(f"{filename} downloaded and MD5 verified.")


def to_postgres():
    """Import the ETHOS.BUILDA CSV files into the database.

    The CSVs carry the four building attributes as JSON objects
    ``{"value", "source", "lineage"}``, so they are streamed into an unlogged
    staging table with ``jsonb`` columns first and unpacked from there. Only
    ``value`` is kept as a column; of the four lineages, two are constant
    across the whole dataset (documented in the metadata) and two are
    retained, because ``source`` determines ``lineage`` uniquely.

    The regions are loaded one at a time and the staging table is truncated in
    between, which bounds its size to the largest single CSV instead of the
    full 7.5 GB.
    """
    target_table = EthosBuilda.targets.tables["ethos_builda_buildings"]
    staging_table = f"{EthosBuilda.schema}.ethos_builda_staging"

    db.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {EthosBuilda.schema};")
    db.execute_sql(f"DROP TABLE IF EXISTS {target_table} CASCADE;")
    db.execute_sql(
        f"""
        CREATE TABLE {target_table} (
            id                        text PRIMARY KEY,
            nuts1                     text,
            geom_point                geometry(Point, 3035),
            construction_year         int,
            construction_year_lineage text,
            size_class                text,
            size_class_lineage        text,
            refurbishment_state       text,
            tabula_type               text
        );
        """
    )

    db.execute_sql(f"DROP TABLE IF EXISTS {staging_table};")
    db.execute_sql(
        f"""
        CREATE UNLOGGED TABLE {staging_table} (
            id                  text,
            position            text,
            construction_year   jsonb,
            size_class          jsonb,
            refurbishment_state jsonb,
            tabula_type         jsonb
        );
        """
    )

    for nuts1 in nuts1_regions():
        csv_file = (
            DOWNLOAD_DIRECTORY / EthosBuilda.targets.files[nuts1]
        ).resolve()
        logger.info(f"Importing {csv_file.name}...")
        copy_csv_to_staging(csv_file, staging_table)

        # ETHOS positions are WKT points in EPSG:3035 already, no reprojection
        db.execute_sql(
            f"""
            INSERT INTO {target_table}
            SELECT
                id,
                substring(id, 1, 3) AS nuts1,
                ST_GeomFromText(position, 3035) AS geom_point,
                (construction_year ->> 'value')::int AS construction_year,
                construction_year ->> 'lineage' AS construction_year_lineage,
                size_class ->> 'value' AS size_class,
                size_class ->> 'lineage' AS size_class_lineage,
                refurbishment_state ->> 'value' AS refurbishment_state,
                tabula_type ->> 'value' AS tabula_type
            FROM {staging_table};
            """
        )
        count = count_rows(target_table, f"nuts1 = '{nuts1}'")
        logger.info(f"{nuts1}: {count} buildings imported.")
        db.execute_sql(f"TRUNCATE {staging_table};")

    db.execute_sql(f"DROP TABLE {staging_table};")
    db.execute_sql(f"CREATE INDEX ON {target_table} USING gist (geom_point);")
    db.execute_sql(f"ANALYZE {target_table};")

    logger.info(
        f"ETHOS.BUILDA imported: {count_rows(target_table)} buildings."
    )


def count_rows(table, condition=None):
    """Count rows of a table, for the import log.

    Parameters
    ----------
    table : str
        Schema-qualified table name.
    condition : str, optional
        A ``WHERE`` clause without the keyword.

    Returns
    -------
    int
        Number of rows.
    """
    query = f"SELECT count(*) FROM {table}"
    if condition is not None:
        query += f" WHERE {condition}"
    with db.session_scope() as session:
        return session.execute(text(query)).scalar()


def copy_csv_to_staging(csv_file, staging_table):
    """Stream one CSV into the staging table using ``psql``'s ``\\copy``.

    ``\\copy`` runs client side, so the file does not have to be visible
    inside the database container. Reading the CSV into pandas first was
    measured at a ~12.9 GB memory peak and discarded.

    Parameters
    ----------
    csv_file : pathlib.Path
        Absolute path of the CSV to import.
    staging_table : str
        Schema-qualified name of the staging table.
    """
    docker_db_config = db.credentials()

    subprocess.run(
        [
            "psql",
            "-h",
            f"{docker_db_config['HOST']}",
            "-p",
            f"{docker_db_config['PORT']}",
            "-d",
            f"{docker_db_config['POSTGRES_DB']}",
            "-U",
            f"{docker_db_config['POSTGRES_USER']}",
            "-v",
            "ON_ERROR_STOP=1",
            "-c",
            rf"\copy {staging_table} FROM '{csv_file}' "
            r"WITH (FORMAT csv, HEADER true)",
        ],
        env={
            **os.environ,
            "PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"],
        },
    )


class EthosBuilda(Dataset):
    """
    Download ETHOS.BUILDA building data and write it to the database.

    ETHOS.BUILDA v2.0.0 (DOI `10.5281/zenodo.13771740
    <https://doi.org/10.5281/zenodo.13771740>`_) is a synthetic building stock
    for Germany: one point per residential building, with construction year,
    size class, refurbishment state and TABULA type. It is published under
    ODbL-1.0 and requires attribution of TABULA.

    Raw data acquisition is kept separate from processing, mirroring
    :py:class:`OpenStreetMap <egon.data.datasets.osm.OpenStreetMap>` and
    :py:class:`OsmBuildingsStreets
    <egon.data.datasets.osm_buildings_streets.OsmBuildingsStreets>`, so the
    ETHOS attributes are available to other datasets as well.

    *Dependencies*
      * :py:func:`Setup <egon.data.datasets.database.setup>`

    *Resulting Tables*
      * society.egon_ethos_builda_buildings is created and filled (table has
        no associated python class)

    **Details and Steps**

    * Download one CSV per NUTS-1 region from Zenodo, verifying the published
      MD5 checksum. In test mode only Schleswig-Holstein (``DEF``) is fetched.
    * Import them via a staging table, unpacking the ``value`` of the four
      JSON-encoded attributes and building the point geometry from the WKT
      ``position`` column, which is EPSG:3035 already.
    """

    #:
    name: str = "EthosBuilda"
    #:
    version: str = "0.0.1"

    schema: str = "society"

    sources = DatasetSources(
        urls={
            nuts1: ZENODO_FILE_URL.format(
                record=ZENODO_RECORD, filename=f"{nuts1}.csv"
            )
            for nuts1 in ETHOS_FILES
        }
    )
    targets = DatasetTargets(
        files={nuts1: f"{nuts1}.csv" for nuts1 in ETHOS_FILES},
        tables={
            "ethos_builda_buildings": "society.egon_ethos_builda_buildings"
        },
    )

    def __init__(self, dependencies):
        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=(download, to_postgres),
        )
