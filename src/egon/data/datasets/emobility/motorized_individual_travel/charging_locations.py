"""
Import of the delivered charging locations and their grid assignment
(new methodology).

The charging locations are generated together with the vehicles and their
events, and every charging event of every vehicle is placed at one of them
(:mod:`.mit_import`). They are the input of the eTraGo model: each location
contributes to the model of the grid it connects to.

Per scenario, :func:`import_charging_locations`

1. stages the delivered geoparquet and fails on duplicate `location_id`s,
2. in test mode keeps only the locations inside the dataset boundary,
3. groups locations at exactly identical coordinates into one **grid
   connection** and derives its **voltage level** from the summed installed
   capacity,
4. assigns every grid connection an MV grid district -- point-in-polygon,
   the nearest district as fallback -- and an eTraGo AC bus: the district's
   for voltage levels 3-7, the EHV substation's for levels 1-2,
5. sets the model type: the district model for levels 4-7, a transmission
   model of its own for levels 1-3.

Assignment works on grid connections rather than locations, so that all
locations at one point share district, bus and model even where the point
lies exactly on a district border.
"""

import io

from loguru import logger
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from egon.data import config, db
from egon.data.datasets.emobility.mit_lgv_input_data import (
    input_file,
    is_legacy_scenario,
)
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvMitLgvChargingLocation,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    DISTRICT_MODEL_MIN_LEVEL,
    MODEL_DISTRICT,
    MODEL_TRANSMISSION,
    voltage_level_sql,
)

#: Staging tables. The delivered geoparquet carries plain WKB without an
#: SRID, which is set when the staged rows are cast.
STAGING_TABLE = "demand.egon_ev_mit_lgv_charging_location_staging"
CONNECTION_TABLE = "demand.egon_ev_mit_lgv_grid_connection_staging"

#: Rows read from the geoparquet before one `COPY` is issued.
CHUNK_SIZE = 500_000

#: Voltage levels connected to an EHV substation rather than to the HV/MV
#: substation of the MV grid district.
EHV_LEVELS = (1, 2)


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


def _qualified(orm_class) -> str:
    """Qualified table name of an ORM class."""
    return f"{orm_class.__table__.schema}.{orm_class.__table__.name}"


def import_charging_locations() -> None:
    """Import the delivered charging locations of all new scenarios and
    assign them to the grid, cf. the module docstring.
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
        staged = _stage(scenario_name)
        _check_unique_location_ids(scenario_name)
        _cast_geometries()
        if not testmode_off():
            _keep_boundary()
        _build_grid_connections()
        _assign_mv_grid_districts()
        _assign_buses()
        _write(scenario_name)
        _log_summary(scenario_name, staged)


def _stage(scenario_name: str) -> int:
    """Stage the delivered geoparquet of one scenario.

    Returns
    -------
    int
        Number of delivered locations
    """
    parquet_file = pq.ParquetFile(
        input_file(scenario_name, "ev_charging_location")
    )
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
    staged = 0
    try:
        statement = (
            f"COPY {STAGING_TABLE} ({', '.join(columns)}) "
            f"FROM STDIN WITH (FORMAT CSV)"
        )
        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE):
            locations = pa.Table.from_batches([batch])
            staged += locations.num_rows

            # PostGIS reads WKB as hex.
            geometry = pa.array(
                [_.hex() for _ in locations.column("geometry").to_pylist()],
                type=pa.string(),
            )
            arrow_table = pa.table(
                {
                    **{
                        _: locations.column(_)
                        for _ in columns
                        if _ != "geom_wkb_hex"
                    },
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
            logger.info(f"  {staged} locations staged...")
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()

    return staged


def _check_unique_location_ids(scenario_name: str) -> None:
    """Fail if a `location_id` is delivered more than once.

    Delivery v1.5 shipped duplicates, which were silently dropped then.
    A delivery with the defect is now rejected, so it cannot pass
    unnoticed.
    """
    duplicates = db.select_dataframe(
        f"""
        SELECT location_id, COUNT(*) AS n
        FROM {STAGING_TABLE}
        GROUP BY location_id
        HAVING COUNT(*) > 1
        ORDER BY location_id
        """
    )
    if not duplicates.empty:
        raise ValueError(
            f"{len(duplicates)} location_id(s) of scenario "
            f"'{scenario_name}' are delivered more than once "
            f"(sample: {duplicates.location_id.head(20).to_list()}). "
            f"location_id must be unique; the delivery has to be fixed."
        )


def _cast_geometries() -> None:
    """Cast the staged WKB to points in EPSG:3035."""
    logger.info("  Casting geometries...")
    db.execute_sql(
        f"""
        ALTER TABLE {STAGING_TABLE}
            ADD COLUMN geom geometry(Point, 3035),
            ADD COLUMN grid_connection_id integer;

        UPDATE {STAGING_TABLE}
        SET geom = ST_SetSRID(
            ST_GeomFromWKB(decode(geom_wkb_hex, 'hex')), 3035
        );

        ALTER TABLE {STAGING_TABLE} DROP COLUMN geom_wkb_hex;
        CREATE INDEX ON {STAGING_TABLE} USING gist (geom);
        ANALYZE {STAGING_TABLE};
        """
    )


def _keep_boundary() -> None:
    """Test mode: drop the locations outside the dataset boundary.

    Filtered by the boundary polygon *before* the grid district
    assignment: in test mode the grid districts cover the boundary only,
    so the nearest-district fallback would otherwise pull every German
    location into a district of the boundary.
    """
    db.execute_sql(
        f"""
        ALTER TABLE {STAGING_TABLE} ADD COLUMN inside boolean;

        UPDATE {STAGING_TABLE} s
        SET inside = TRUE
        FROM boundaries.vg250_sta_union b
        WHERE ST_Intersects(b.geometry, s.geom);

        DELETE FROM {STAGING_TABLE} WHERE inside IS NOT TRUE;
        ALTER TABLE {STAGING_TABLE} DROP COLUMN inside;
        """
    )


def _build_grid_connections() -> None:
    """Group locations at identical coordinates into grid connections.

    Exact coordinate equality: locations at different points stay
    separate however close they are. The voltage level follows from the
    summed installed capacity of the connection.
    """
    logger.info("  Building grid connections...")
    capacity_mw = "capacity_mw"
    db.execute_sql(
        f"""
        UPDATE {STAGING_TABLE} s
        SET grid_connection_id = r.grid_connection_id
        FROM (
            SELECT
                location_id,
                DENSE_RANK() OVER (ORDER BY ST_X(geom), ST_Y(geom))::integer
                    AS grid_connection_id
            FROM {STAGING_TABLE}
        ) r
        WHERE r.location_id = s.location_id;

        DROP TABLE IF EXISTS {CONNECTION_TABLE};
        CREATE UNLOGGED TABLE {CONNECTION_TABLE} AS
        SELECT
            grid_connection_id,
            (ARRAY_AGG(geom))[1] AS geom,
            SUM(
                charging_points::double precision
                * average_charging_capacity
            ) / 1000.0 AS {capacity_mw}
        FROM {STAGING_TABLE}
        GROUP BY grid_connection_id;

        ALTER TABLE {CONNECTION_TABLE}
            ADD COLUMN voltage_level smallint,
            ADD COLUMN mv_grid_id integer,
            ADD COLUMN assignment text,
            ADD COLUMN bus_id integer,
            ADD COLUMN model_type text;

        UPDATE {CONNECTION_TABLE}
        SET voltage_level = {voltage_level_sql(capacity_mw)};

        UPDATE {CONNECTION_TABLE}
        SET model_type = CASE
            WHEN voltage_level >= {DISTRICT_MODEL_MIN_LEVEL}
                THEN '{MODEL_DISTRICT}'
            ELSE '{MODEL_TRANSMISSION}'
        END;

        CREATE INDEX ON {CONNECTION_TABLE} USING gist (geom);
        CREATE INDEX ON {CONNECTION_TABLE} (grid_connection_id);
        ANALYZE {CONNECTION_TABLE};
        """
    )


def _assign_mv_grid_districts() -> None:
    """Assign every grid connection an MV grid district.

    Point-in-polygon first. The few connections outside every district --
    on the coast, in slivers between polygons -- go to the nearest
    district, so that no placement and no charging energy is lost.
    """
    logger.info("  Assigning grid districts...")
    # Driven by the ~3,800 grid districts against the spatial index on the
    # connections: there is no spatial index on
    # `grid.egon_mv_grid_district`. `UPDATE ... FROM` keeps a point on the
    # border of two districts a single row.
    db.execute_sql(
        f"""
        UPDATE {CONNECTION_TABLE} c
        SET mv_grid_id = mvgd.bus_id, assignment = 'within'
        FROM grid.egon_mv_grid_district mvgd
        WHERE ST_Intersects(mvgd.geom, c.geom);

        UPDATE {CONNECTION_TABLE} c
        SET mv_grid_id = nearest.bus_id, assignment = 'nearest'
        FROM (
            SELECT
                c2.grid_connection_id,
                (
                    SELECT mvgd.bus_id
                    FROM grid.egon_mv_grid_district mvgd
                    ORDER BY ST_Distance(mvgd.geom, c2.geom)
                    LIMIT 1
                ) AS bus_id
            FROM {CONNECTION_TABLE} c2
            WHERE c2.mv_grid_id IS NULL
        ) nearest
        WHERE nearest.grid_connection_id = c.grid_connection_id;
        """
    )


def _assign_buses() -> None:
    """Assign every grid connection the eTraGo AC bus of its model.

    Voltage levels 3-7 connect to the HV/MV substation of their MV grid
    district, whose `bus_id` is the district's. Levels 1-2 connect to the
    EHV substation whose Voronoi cell they lie in, or the nearest one, as
    power plants and public bus depots do.
    """
    ehv_levels = ", ".join(str(_) for _ in EHV_LEVELS)
    db.execute_sql(
        f"""
        UPDATE {CONNECTION_TABLE}
        SET bus_id = mv_grid_id
        WHERE voltage_level NOT IN ({ehv_levels});

        UPDATE {CONNECTION_TABLE} c
        SET bus_id = v.bus_id
        FROM grid.egon_ehv_substation_voronoi v
        WHERE c.voltage_level IN ({ehv_levels})
            AND ST_Intersects(v.geom, ST_Transform(c.geom, 4326));

        UPDATE {CONNECTION_TABLE} c
        SET bus_id = (
            SELECT v.bus_id
            FROM grid.egon_ehv_substation_voronoi v
            ORDER BY ST_Distance(v.geom, ST_Transform(c.geom, 4326))
            LIMIT 1
        )
        WHERE c.voltage_level IN ({ehv_levels}) AND c.bus_id IS NULL;
        """
    )


def _write(scenario_name: str) -> None:
    """Write the assigned locations of one scenario to the target table."""
    table = _qualified(EgonEvMitLgvChargingLocation)
    db.execute_sql(
        f"""
        DELETE FROM {table} WHERE scenario = '{scenario_name}';

        INSERT INTO {table} (
            location_id, scenario, charging_points,
            average_charging_capacity, use_case, candidate_uid,
            is_synthetic_location, mv_grid_id, grid_connection_id,
            voltage_level, model_type, bus_id, assignment, geom)
        SELECT
            s.location_id,
            '{scenario_name}',
            s.charging_points,
            s.average_charging_capacity,
            s.use_case,
            s.candidate_uid,
            s.is_synthetic_location,
            c.mv_grid_id,
            s.grid_connection_id,
            c.voltage_level,
            c.model_type,
            c.bus_id,
            c.assignment,
            s.geom
        FROM {STAGING_TABLE} s
        JOIN {CONNECTION_TABLE} c USING (grid_connection_id);
        """
    )

    index = f"idx_{table.split('.')[-1]}_geom"
    db.execute_sql(
        f"CREATE INDEX IF NOT EXISTS {index} ON {table} USING gist (geom);"
    )


def _log_summary(scenario_name: str, staged: int) -> None:
    """Log what the import and the assignment did, then drop staging."""
    table = _qualified(EgonEvMitLgvChargingLocation)
    summary = db.select_dataframe(
        f"""
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE is_synthetic_location) AS synthetic,
            COUNT(DISTINCT grid_connection_id) AS connections,
            COUNT(*) FILTER (
                WHERE average_charging_capacity = 0
            ) AS zero_capacity
        FROM {table}
        WHERE scenario = '{scenario_name}'
        """
    ).iloc[0]
    fallback = db.select_dataframe(
        f"""
        SELECT
            COUNT(*) AS n,
            COUNT(*) FILTER (WHERE s.is_synthetic_location) AS synthetic,
            COALESCE(MAX(ST_Distance(mvgd.geom, s.geom)), 0) AS max_m
        FROM {table} s
        JOIN grid.egon_mv_grid_district mvgd ON mvgd.bus_id = s.mv_grid_id
        WHERE s.scenario = '{scenario_name}' AND s.assignment = 'nearest'
        """
    ).iloc[0]
    models = db.select_dataframe(
        f"""
        SELECT
            model_type,
            voltage_level,
            COUNT(*) AS locations,
            COUNT(DISTINCT grid_connection_id) AS connections
        FROM {table}
        WHERE scenario = '{scenario_name}'
        GROUP BY model_type, voltage_level
        ORDER BY voltage_level
        """
    )

    logger.info(
        f"  {staged} locations delivered, {int(summary.n)} written "
        f"({int(summary.synthetic)} synthetic, {int(summary.zero_capacity)} "
        f"with zero average charging capacity) at "
        f"{int(summary.connections)} grid connections."
    )
    logger.info(
        f"  {int(fallback.n)} locations outside every MV grid district "
        f"assigned to the nearest one ({int(fallback.synthetic)} synthetic, "
        f"at most {float(fallback.max_m):.0f} m away)."
    )
    logger.info(
        "  Locations and grid connections per voltage level: "
        f"{models.to_dict('records')}"
    )

    db.execute_sql(
        f"""
        DROP TABLE IF EXISTS {STAGING_TABLE};
        DROP TABLE IF EXISTS {CONNECTION_TABLE};
        """
    )
