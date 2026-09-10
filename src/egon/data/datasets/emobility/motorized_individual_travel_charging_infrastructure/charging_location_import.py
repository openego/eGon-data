"""
Import of the delivered charging locations (new methodology).

The charging infrastructure of the new methodology is generated together
with the vehicles and their events, so the two are mutually consistent.
This module imports the delivered sites into
`demand.egon_ev_mit_lgv_charging_location` and assigns each of them an MV
grid district.

The mapping of charging events to charging locations
(`ev_mapping_event_location.parquet`) is **not** imported, cf.
:mod:`egon.data.datasets.emobility.motorized_individual_travel.mit_import`.
Charging locations carry their own `use_case`, so no consumer has to
aggregate that mapping to find out what a site is for.
"""

import io

from loguru import logger
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from egon.data import config, db
from egon.data.datasets.emobility.mit_lgv_input_data import (
    download_and_extract_scenario,
    input_file,
    is_legacy_scenario,
)
from egon.data.datasets.emobility.motorized_individual_travel_charging_infrastructure.db_classes import (  # noqa: E501
    EgonEvMitLgvChargingLocation,
)

#: Staging table the delivered geometries land in before they are cast
#: to EPSG:3035 geometries. The delivered geoparquet carries plain WKB
#: without an SRID.
STAGING_TABLE = "demand.egon_ev_mit_lgv_charging_location_staging"

#: Rows read from the geoparquet before one `COPY` is issued.
CHUNK_SIZE = 500_000


def new_scenarios() -> list:
    """Configured scenarios that use the new methodology."""
    return [
        _
        for _ in config.settings()["egon-data"]["--scenarios"]
        if not is_legacy_scenario(_)
    ]


def testmode_off() -> bool:
    """Whether the pipeline runs on the full German dataset."""
    return config.settings()["egon-data"]["--dataset-boundary"] == "Everything"


def download_input_data() -> None:
    """Download and extract the input data of the new scenarios.

    The charging infrastructure dataset is independent of the MIT
    dataset, so it fetches the shared archive itself. Both are
    idempotent and serialise against each other with a lock file, so
    whichever runs first does the work.
    """
    for scenario_name in new_scenarios():
        logger.info(f"Scenario {scenario_name}: getting input data...")
        download_and_extract_scenario(scenario_name)


def import_charging_locations() -> None:
    """Import the delivered charging locations of all new scenarios.

    Each location is assigned an `mv_grid_id` by a point-in-polygon join
    against `grid.egon_mv_grid_district`. Locations outside every grid
    district keep `NULL` and are logged rather than dropped -- except in
    test mode, where the grid districts cover the dataset boundary only
    and everything outside it is exactly what has to go.
    """
    scenarios = new_scenarios()
    if not scenarios:
        logger.info(
            "No scenario on the new methodology configured, no charging "
            "locations to import."
        )
        return

    for scenario_name in scenarios:
        logger.info(f"Scenario {scenario_name}: importing locations...")
        _import_scenario(scenario_name)


def _import_scenario(scenario_name: str) -> None:
    """Import the charging locations of one scenario."""
    source = input_file(scenario_name, "ev_charging_location")
    parquet_file = pq.ParquetFile(source)

    columns = [
        "location_id",
        "charging_points",
        "average_charging_capacity",
        "use_case",
        "candidate_uid",
        "is_synthetic_location",
        "geom_wkb_hex",
    ]

    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {STAGING_TABLE};
        CREATE UNLOGGED TABLE {STAGING_TABLE} (
            location_id bigint,
            charging_points integer,
            average_charging_capacity integer,
            use_case text,
            candidate_uid text,
            is_synthetic_location boolean,
            geom_wkb_hex text
        );
        """
    )

    connection = db.engine().raw_connection()
    imported = 0
    try:
        statement = (
            f"COPY {STAGING_TABLE} ({', '.join(columns)}) "
            f"FROM STDIN WITH (FORMAT CSV)"
        )
        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
            locations = pa.Table.from_batches([batch])
            imported += locations.num_rows

            # PostGIS reads WKB as hex; the delivered geoparquet carries
            # plain WKB without an SRID, which is set when the staging
            # rows are cast below.
            geometry = pa.array(
                [_.hex() for _ in locations.column("geometry").to_pylist()],
                type=pa.string(),
            )
            arrow_table = pa.table(
                {
                    "location_id": locations.column("location_id"),
                    "charging_points": locations.column("charging_points"),
                    "average_charging_capacity": locations.column(
                        "average_charging_capacity"
                    ),
                    "use_case": locations.column("use_case"),
                    "candidate_uid": locations.column("candidate_uid"),
                    "is_synthetic_location": locations.column(
                        "is_synthetic_location"
                    ),
                    "geom_wkb_hex": geometry,
                }
            )

            buffer = pa.BufferOutputStream()
            pacsv.write_csv(
                arrow_table,
                buffer,
                pacsv.WriteOptions(include_header=False),
            )
            with connection.cursor() as cursor:
                cursor.copy_expert(
                    statement,
                    io.BytesIO(buffer.getvalue().to_pybytes()),
                )
            logger.info(f"  {imported} locations staged...")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    table = (
        f"{EgonEvMitLgvChargingLocation.__table__.schema}."
        f"{EgonEvMitLgvChargingLocation.__table__.name}"
    )

    logger.info("  Casting geometries...")
    db.execute_sql(
        f"""
        ALTER TABLE {STAGING_TABLE}
            ADD COLUMN geom geometry(Point, 3035),
            ADD COLUMN mv_grid_id integer;

        UPDATE {STAGING_TABLE}
        SET geom = ST_SetSRID(
            ST_GeomFromWKB(decode(geom_wkb_hex, 'hex')), 3035
        );

        CREATE INDEX ON {STAGING_TABLE} USING gist (geom);
        ANALYZE {STAGING_TABLE};
        """
    )

    logger.info("  Assigning grid districts...")
    # Driven by the ~3,800 grid districts against the spatial index on
    # the staged points, not the other way round: there is no spatial
    # index on `grid.egon_mv_grid_district`, so a per-point lookup would
    # scan every polygon for every one of ~2 million locations. `UPDATE
    # ... FROM` also keeps a point that touches two grid district
    # boundaries a single row, which a join would duplicate.
    db.execute_sql(
        f"""
        UPDATE {STAGING_TABLE} s
        SET mv_grid_id = mvgd.bus_id
        FROM grid.egon_mv_grid_district mvgd
        WHERE ST_Intersects(mvgd.geom, s.geom);
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM {table} WHERE scenario = '{scenario_name}';

        INSERT INTO {table} (
            location_id, scenario, charging_points,
            average_charging_capacity, use_case, candidate_uid,
            is_synthetic_location, mv_grid_id, geom)
        SELECT
            location_id,
            '{scenario_name}',
            charging_points,
            average_charging_capacity,
            use_case,
            candidate_uid,
            is_synthetic_location,
            mv_grid_id,
            geom
        FROM {STAGING_TABLE};

        DROP TABLE IF EXISTS {STAGING_TABLE};
        """
    )

    _log_and_filter(scenario_name, table, imported)


def _log_and_filter(scenario_name: str, table: str, staged: int) -> None:
    """Report and, in test mode, drop the locations outside the boundary.

    `is_synthetic_location` is reported separately: in delivery v1.4 all
    synthetic sites are `highway_fast` municipality centroids generated
    as a fallback where a municipality has no real candidate, and
    consumers placing high power charging infrastructure need to be able
    to tell them from real sites.
    """
    outside = db.select_dataframe(
        f"""
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE is_synthetic_location) AS synthetic
        FROM {table}
        WHERE scenario = '{scenario_name}' AND mv_grid_id IS NULL
        """
    ).iloc[0]

    total = db.select_dataframe(
        f"""
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE is_synthetic_location) AS synthetic,
            COUNT(*) FILTER (
                WHERE average_charging_capacity = 0
            ) AS zero_capacity
        FROM {table}
        WHERE scenario = '{scenario_name}'
        """
    ).iloc[0]

    logger.info(
        f"  {staged} locations delivered, {int(total.n)} rows written "
        f"({int(total.synthetic)} synthetic, "
        f"{int(total.zero_capacity)} with zero average charging "
        f"capacity)."
    )
    logger.info(
        f"  {int(outside.n)} locations lie outside every MV grid "
        f"district ({int(outside.synthetic)} of them synthetic)."
    )

    if testmode_off():
        _create_geometry_index(table)
        return

    # In test mode the grid districts cover the dataset boundary only,
    # so "outside every grid district" is exactly "outside the region".
    db.execute_sql(
        f"""
        DELETE FROM {table}
        WHERE scenario = '{scenario_name}' AND mv_grid_id IS NULL
        """
    )
    logger.info(
        f"  Test mode: dropped {int(outside.n)} locations outside the "
        f"dataset boundary."
    )

    _create_geometry_index(table)


def _create_geometry_index(table: str) -> None:
    """Add the spatial index consumers of the locations need."""
    index = f"idx_{table.split('.')[-1]}_geom"
    db.execute_sql(
        f"CREATE INDEX IF NOT EXISTS {index} ON {table} USING gist (geom);"
    )
