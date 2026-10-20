"""
Import of the delivered M1+N1 input data (new methodology).

The new methodology no longer derives the electric vehicle fleet from
KBA registration statistics. One bundle per scenario is delivered as
parquet and imported into the database. The unit of the eTraGo model is
the **placement** -- one vehicle's realisation of one charging event at
one charging location (ADR-0001) -- so the import is organised around
the charging locations:

Call order::

    download_and_extract()
    charging_locations.import_charging_locations()
    import_location_events()      # placements, one array per location
    import_charging_events()      # only the events placed somewhere
    import_ev_pool()              # only the profiles of those events
    import_ev_counts()

In test mode the selection cascades along that order: locations inside
the boundary, their placements, the events those reference and the
profiles of those events. The result is exactly what a Germany-wide run
would give for the districts inside the boundary.

Large files are read in row group chunks with pyarrow and streamed into
PostgreSQL via ``COPY ... FROM STDIN``.
"""

from contextlib import contextmanager
import io
import math
import struct

from sqlalchemy import func
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.emobility.mit_lgv_input_data import (
    download_and_extract_scenario,
    input_file,
)
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvMitLgvChargingLocation,
    EgonEvMitLgvCountMunicipality,
    EgonEvMitLgvLocationEvents,
    EgonEvMitLgvPool,
    EgonEvMitLgvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    EV_TYPES,
    EVENT_COLUMN_ALIASES,
    EVENT_COLUMN_MAPPING,
    is_legacy_scenario,
    read_geolis_metadata_file,
    read_simbev_metadata_file,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters
from egon.data.datasets.zensus_vg250 import Vg250Gem

#: Rows read from `ev_event.parquet` before one `COPY` is issued. Row
#: groups of the delivery are ~750,000 rows, so this reads one group at
#: a time.
EVENT_CHUNK_SIZE = 1_000_000

#: Rows read from `ev_mapping_event_location.parquet` per batch.
PLACEMENT_BATCH_SIZE = 50_000_000

#: Size of the sort keys of one pass of the placement import. One
#: placement costs 8 bytes (one int64 key), so 8 GB fold 1 bn placements
#: per pass over the file and the ~5 bn of a reGon scenario take five
#: passes. Peak memory is about three times this, during the split into
#: arrays.
PLACEMENT_MEMORY_BUDGET_BYTES = 8 * 10**9

#: Arrays written per binary `COPY`, and the element budget of one COPY.
ARRAY_COPY_ROWS = 200_000
ARRAY_COPY_ELEMENTS = 50_000_000


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


def scenario_variation(scenario_name: str) -> str:
    """Scenario variation name of a scenario, cf. the dataset sources."""
    sources, _ = load_sources_and_targets("MotorizedIndividualTravel")
    return sources.files["original_data"]["scenario"]["variation"][
        scenario_name
    ]


def boundary_ags():
    """AGS of the municipalities inside the dataset boundary.

    Returns
    -------
    numpy.ndarray or None
        The AGS inside the boundary, or None when the pipeline runs on
        all of Germany and no filter is needed.
    """
    if testmode_off():
        return None

    with db.session_scope() as session:
        query = session.query(func.distinct(Vg250Gem.ags).label("ags"))
    ags = pd.read_sql(query.statement, query.session.bind)

    return np.sort(ags.ags.astype("int64").unique())


@contextmanager
def _copy_target(table: str, columns: list):
    """Yield a function streaming arrow tables into one target table.

    The connection is held open for all chunks of one file, so a 96 row
    group import does not open 96 connections.

    Parameters
    ----------
    table : str
        Qualified target table name
    columns : list of str
        Target columns; the arrow tables handed to the yielded function
        must have exactly this column order.
    """
    statement = (
        f"COPY {table} ({', '.join(columns)}) " f"FROM STDIN WITH (FORMAT CSV)"
    )
    connection = db.engine().raw_connection()

    def write(arrow_table: pa.Table) -> None:
        if arrow_table.num_rows == 0:
            return
        buffer = pa.BufferOutputStream()
        pacsv.write_csv(
            arrow_table, buffer, pacsv.WriteOptions(include_header=False)
        )
        # pyarrow writes NULL as an unquoted empty field and the empty
        # string as `""`, which is exactly what PostgreSQL's CSV format
        # expects, so `use_case` keeps its NULLs.
        stream = io.BytesIO(buffer.getvalue().to_pybytes())
        with connection.cursor() as cursor:
            cursor.copy_expert(statement, stream)

    try:
        yield write
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def _copy_arrow_table(arrow_table: pa.Table, table: str, columns: list):
    """Stream a single arrow table into PostgreSQL."""
    with _copy_target(table, columns) as write:
        write(arrow_table)


def _is_in(column, values):
    """Membership mask, with the value set cast to the column type.

    The delivered files use sized dtypes (`ags` and `ev_id` are int32)
    while the values selected from the database or from numpy come back
    as int64. `pyarrow.compute.is_in` does not always reconcile the two
    on its own.
    """
    value_set = pa.array(values)
    if value_set.type != column.type:
        value_set = value_set.cast(column.type)
    return pc.is_in(column, value_set=value_set)


def _constant_column(value, length: int) -> pa.Array:
    """An arrow array repeating `value` `length` times."""
    return pa.array([value] * length, type=pa.string())


def _qualified(orm_class) -> str:
    """Qualified table name of an ORM class."""
    return f"{orm_class.__table__.schema}.{orm_class.__table__.name}"


def _clear_scenario(table: str, scenario_name: str) -> None:
    """Remove a scenario's rows before (re-)writing them.

    Without this a retried task would append to what the failed attempt
    already wrote and hit the primary key instead of recovering.
    """
    db.execute_sql(f"DELETE FROM {table} WHERE scenario = '{scenario_name}';")


def download_and_extract() -> None:
    """Download and extract the input data of all new-methodology
    scenarios.

    Skips the download when the archive is already present and the
    extraction when the scenario directory is already complete, so a
    re-run of this task does not re-fetch several GB.
    """
    scenarios = new_scenarios()
    if not scenarios:
        print(
            "No scenario on the new methodology configured, nothing to "
            "download."
        )
        return

    for scenario_name in scenarios:
        print(f"SCENARIO: {scenario_name}")
        directory = download_and_extract_scenario(scenario_name)
        print(f"  Input data ready in {directory}.")


def import_ev_counts() -> None:
    """Import `ev_count_municipality.parquet`.

    Writes `demand.egon_ev_mit_lgv_count_municipality` and logs the
    two-way AGS difference against the VG250 municipalities as well as
    the delivered fleet total against the scenario parameter.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        ags = boundary_ags()

        delivered = pq.read_table(
            input_file(scenario_name, "ev_count_municipality")
        )
        missing_columns = [
            _ for _ in EV_TYPES if _ not in delivered.column_names
        ]
        if missing_columns:
            raise ValueError(
                f"ev_count_municipality of scenario '{scenario_name}' "
                f"is missing the vehicle type columns "
                f"{missing_columns}. The delivery does not match the "
                f"agreed taxonomy."
            )

        counts = delivered
        if ags is not None:
            counts = counts.filter(_is_in(counts.column("ags"), ags))

        columns = ["scenario", "scenario_variation", "ags", "rs7_id"] + list(
            EV_TYPES
        )
        variation = scenario_variation(scenario_name)
        arrow_table = pa.table(
            {
                "scenario": _constant_column(scenario_name, counts.num_rows),
                "scenario_variation": _constant_column(
                    variation, counts.num_rows
                ),
                "ags": counts.column("ags"),
                "rs7_id": counts.column("rs7_id"),
                **{_: counts.column(_) for _ in EV_TYPES},
            }
        )

        print(
            f"  Importing vehicle counts for {counts.num_rows} "
            f"municipalities..."
        )
        _clear_scenario(
            _qualified(EgonEvMitLgvCountMunicipality), scenario_name
        )
        _copy_arrow_table(
            arrow_table,
            _qualified(EgonEvMitLgvCountMunicipality),
            columns,
        )

        _log_fleet_total(scenario_name, delivered)


def _log_fleet_total(scenario_name: str, delivered_counts: pa.Table) -> None:
    """Log the delivered fleet total against `parameters.mobility()`.

    The scenario parameter is not a hard benchmark at present and is
    expected to be updated, so this is information only.
    """
    delivered = int(
        sum(pc.sum(delivered_counts.column(_)).as_py() or 0 for _ in EV_TYPES)
    )
    try:
        parameters = get_sector_parameters("mobility", scenario=scenario_name)[
            "motorized_individual_travel"
        ]
        target = sum(_["ev_count"] for _ in parameters.values())
    except (KeyError, TypeError):
        print(f"  Delivered fleet: {delivered} vehicles.")
        return

    deviation = delivered / target - 1 if target else float("nan")
    print(
        f"  Delivered fleet: {delivered} vehicles against "
        f"{target} in parameters.mobility() ({deviation:+.1%}). "
        f"Information only, the parameter is a cross-check."
    )




# ---------------------------------------------------------------------------
# Test mode selection
# ---------------------------------------------------------------------------


def _boundary_location_mask(scenario_name: str):
    """Boolean lookup over the location id space: imported locations.

    In test mode the location table holds only the locations inside the
    boundary (cf. :mod:`.charging_locations`), so this is the boundary
    filter of the placements. On a Germany-wide run no filter is needed.

    Returns
    -------
    numpy.ndarray or None
        `mask[location_id]` is True for locations to keep, or None
    """
    if testmode_off():
        return None
    location_ids = db.select_dataframe(
        f"""
        SELECT location_id
        FROM {_qualified(EgonEvMitLgvChargingLocation)}
        WHERE scenario = '{scenario_name}'
        """
    ).location_id.to_numpy(dtype="int64")
    mask = np.zeros(int(location_ids.max(initial=0)) + 1, dtype=bool)
    mask[location_ids] = True
    return mask


def _referenced_event_ids(scenario_name: str):
    """Charging events placed at an imported location (test mode only).

    Returns
    -------
    numpy.ndarray or None
        Sorted event ids, or None when no filter is needed
    """
    if testmode_off():
        return None
    return np.sort(
        db.select_dataframe(
            f"""
            SELECT DISTINCT UNNEST(event_ids) AS event_id
            FROM {_qualified(EgonEvMitLgvLocationEvents)}
            WHERE scenario = '{scenario_name}'
            """
        ).event_id.to_numpy(dtype="int64")
    )


def _referenced_ev_ids(scenario_name: str):
    """Profiles of the imported charging events (test mode only).

    Returns
    -------
    numpy.ndarray or None
        Sorted pool EV ids, or None when no filter is needed
    """
    if testmode_off():
        return None
    return np.sort(
        db.select_dataframe(
            f"""
            SELECT DISTINCT ev_id
            FROM {_qualified(EgonEvMitLgvTrip)}
            WHERE scenario = '{scenario_name}'
            """
        ).ev_id.to_numpy(dtype="int64")
    )


# ---------------------------------------------------------------------------
# Placements
# ---------------------------------------------------------------------------

#: Binary COPY framing, cf. the PostgreSQL documentation of `COPY`.
_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)
_COPY_TRAILER = struct.pack(">h", -1)
#: OID of PostgreSQL's int4, the element type of `event_ids`.
_INT4_OID = 23


def _array_rows_binary(
    scenario_name: str, location_ids, counts, event_ids
) -> bytes:
    """Encode (scenario, location_id, event_ids) rows for a binary COPY.

    Parameters
    ----------
    scenario_name : str
        Scenario name, the first column of every row
    location_ids : numpy.ndarray
        Location id of each row
    counts : numpy.ndarray
        Number of array elements of each row
    event_ids : numpy.ndarray
        The array elements of all rows, concatenated in row order

    Returns
    -------
    bytes
        Rows in PostgreSQL's binary COPY format, without header/trailer
    """
    scenario = scenario_name.encode()
    prefix = np.zeros(
        len(location_ids),
        dtype=np.dtype(
            [
                ("n_fields", ">i2"),
                ("scenario_length", ">i4"),
                ("scenario", f"S{len(scenario)}"),
                ("location_length", ">i4"),
                ("location_id", ">i8"),
                ("array_length", ">i4"),
                ("n_dims", ">i4"),
                ("has_null", ">i4"),
                ("element_type", ">i4"),
                ("dim_length", ">i4"),
                ("lower_bound", ">i4"),
            ]
        ),
    )
    prefix["n_fields"] = 3
    prefix["scenario_length"] = len(scenario)
    prefix["scenario"] = scenario
    prefix["location_length"] = 8
    prefix["location_id"] = location_ids
    prefix["array_length"] = 20 + 8 * counts
    prefix["n_dims"] = 1
    prefix["element_type"] = _INT4_OID
    prefix["dim_length"] = counts
    prefix["lower_bound"] = 1
    prefix_bytes = prefix.view(np.uint8).reshape(len(location_ids), -1)

    elements = np.empty(
        len(event_ids), dtype=np.dtype([("length", ">i4"), ("value", ">i4")])
    )
    elements["length"] = 4
    elements["value"] = event_ids
    element_bytes = elements.view(np.uint8)
    rows = np.split(element_bytes, np.cumsum(counts[:-1]) * 8)

    parts = [None] * (2 * len(rows))
    parts[0::2] = list(prefix_bytes)
    parts[1::2] = rows
    return np.concatenate(parts).tobytes()


def _copy_arrays(scenario_name: str, location_ids, counts, event_ids) -> None:
    """Write folded placements of one pass with binary COPYs."""
    table = _qualified(EgonEvMitLgvLocationEvents)
    statement = (
        f"COPY {table} (scenario, location_id, event_ids) "
        f"FROM STDIN WITH (FORMAT binary)"
    )
    ends = np.cumsum(counts)
    starts = ends - counts

    connection = db.engine().raw_connection()
    try:
        row = 0
        while row < len(location_ids):
            # Rows of this COPY: at most ARRAY_COPY_ROWS, and at most
            # ARRAY_COPY_ELEMENTS elements unless a single row has more.
            limit = np.searchsorted(
                ends, starts[row] + ARRAY_COPY_ELEMENTS, side="right"
            )
            stop = min(row + ARRAY_COPY_ROWS, max(int(limit), row + 1))
            payload = _array_rows_binary(
                scenario_name,
                location_ids[row:stop],
                counts[row:stop],
                event_ids[starts[row] : ends[stop - 1]],
            )
            with connection.cursor() as cursor:
                cursor.copy_expert(
                    statement,
                    io.BytesIO(_COPY_HEADER + payload + _COPY_TRAILER),
                )
            row = stop
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


def import_location_events() -> None:
    """Fold `ev_mapping_event_location.parquet` into one event-id array per
    charging location (`demand.egon_ev_mit_lgv_location_events`, ADR-0004).

    The delivered file has one row per placement, ordered by vehicle, and
    up to ~5 bn rows. It is folded without temporary files: each pass over
    the file collects the placements of one range of location ids, sorts
    them by one int64 key `location_id << 32 | event_id` in place and
    writes the arrays. The number of passes follows from
    :data:`PLACEMENT_MEMORY_BUDGET_BYTES`.

    In test mode only the placements at the imported locations, i.e.
    inside the boundary, are kept.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        source = pq.ParquetFile(
            input_file(scenario_name, "ev_mapping_event_location")
        )
        n_rows = source.metadata.num_rows
        n_locations = (
            int(
                pc.max(
                    pq.read_table(
                        input_file(scenario_name, "ev_charging_location"),
                        columns=["location_id"],
                    ).column("location_id")
                ).as_py()
            )
            + 1
        )
        keep = _boundary_location_mask(scenario_name)
        if keep is not None:
            keep = np.pad(keep, (0, max(0, n_locations - len(keep))))

        # In test mode the kept share is small; one pass suffices.
        passes = (
            1
            if keep is not None
            else max(
                1, math.ceil(n_rows * 8 / PLACEMENT_MEMORY_BUDGET_BYTES)
            )
        )
        bounds = np.linspace(0, n_locations, passes + 1).astype("int64")
        print(
            f"  Folding {n_rows} placements of {n_locations} location ids "
            f"in {passes} pass(es)..."
        )

        _clear_scenario(_qualified(EgonEvMitLgvLocationEvents), scenario_name)
        written = 0
        for low, high in zip(bounds[:-1], bounds[1:]):
            keys = []
            for batch in source.iter_batches(
                batch_size=PLACEMENT_BATCH_SIZE,
                columns=["event_id", "location_id"],
            ):
                location = batch.column(1).to_numpy().astype("int64")
                selected = (location >= low) & (location < high)
                if keep is not None:
                    selected &= keep[location]
                if not selected.any():
                    continue
                event = batch.column(0).to_numpy().astype("int64")
                keys.append((location[selected] << 32) | event[selected])
            if not keys:
                continue

            key = np.concatenate(keys)
            del keys
            key.sort()
            location = key >> 32
            starts = np.concatenate(
                ([0], np.flatnonzero(location[1:] != location[:-1]) + 1)
            )
            location_ids = location[starts]
            counts = np.diff(np.append(starts, len(location)))
            del location, starts
            event_ids = (key & 0xFFFFFFFF).astype("int32")
            del key

            _copy_arrays(scenario_name, location_ids, counts, event_ids)
            written += len(event_ids)
            print(
                f"    location ids {low}-{high - 1}: {len(location_ids)} "
                f"locations, {len(event_ids)} placements written "
                f"({written} in total)..."
            )
        print(f"  Imported {written} placements.")


# ---------------------------------------------------------------------------
# Events, pool, counts
# ---------------------------------------------------------------------------


def import_charging_events() -> None:
    """Import the charging events of `ev_event.parquet` into
    `demand.egon_ev_mit_lgv_trip`.

    Only events with a charging use case, i.e. charging demand, are
    imported: the model is built from placements, and a placement's
    deficit is its charging demand, so driving and parking events are not
    needed (Q21). In test mode only the events placed at a location inside
    the boundary are imported.

    `charging_use_case` is renamed to `use_case` on import (D12);
    `park_time_timesteps` is not imported. `charging_capacity_grid` is
    filled from the delivered nameplate power, which is grid-side
    (:data:`EVENT_COLUMN_ALIASES`); `charging_capacity_battery` stays
    NULL.
    """
    columns = (
        ["scenario"]
        + list(EVENT_COLUMN_MAPPING.values())
        + list(EVENT_COLUMN_ALIASES.keys())
    )

    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        referenced = _referenced_event_ids(scenario_name)

        pool_ev_ids = set(
            pq.read_table(
                input_file(scenario_name, "ev_pool"), columns=["ev_id"]
            )
            .column("ev_id")
            .to_pylist()
        )

        parquet_file = pq.ParquetFile(input_file(scenario_name, "ev_event"))
        imported = 0
        orphans = set()

        _clear_scenario(_qualified(EgonEvMitLgvTrip), scenario_name)
        with _copy_target(_qualified(EgonEvMitLgvTrip), columns) as write:
            for batch in parquet_file.iter_batches(
                batch_size=EVENT_CHUNK_SIZE,
                columns=list(EVENT_COLUMN_MAPPING.keys()),
            ):
                events = pa.Table.from_batches([batch])
                events = events.filter(
                    pc.is_valid(events.column("charging_use_case"))
                )
                if referenced is not None:
                    events = events.filter(
                        _is_in(events.column("event_id"), referenced)
                    )
                if events.num_rows == 0:
                    continue

                orphans.update(
                    set(pc.unique(events.column("ev_id")).to_pylist())
                    - pool_ev_ids
                )

                imported += events.num_rows
                write(
                    pa.table(
                        {
                            "scenario": _constant_column(
                                scenario_name, events.num_rows
                            ),
                            **{
                                target: events.column(delivered)
                                for delivered, target in (
                                    EVENT_COLUMN_MAPPING.items()
                                )
                            },
                            **{
                                target: events.column(delivered)
                                for target, delivered in (
                                    EVENT_COLUMN_ALIASES.items()
                                )
                            },
                        }
                    )
                )

        print(f"  Imported {imported} charging events.")
        if orphans:
            raise ValueError(
                f"{len(orphans)} ev_id values of charging events of "
                f"scenario '{scenario_name}' are not in ev_pool (sample: "
                f"{sorted(orphans)[:10]}). Their vehicle type, and so "
                f"their battery capacity, is unknown."
            )


def import_ev_pool() -> None:
    """Import `ev_pool.parquet` into `demand.egon_ev_mit_lgv_pool`.

    Adds `n_vehicles`, the number of vehicles instantiated from each
    profile across Germany, counted from
    `ev_mapping_ev_municipality.parquet`. The municipality mapping itself
    is not imported any more: the model is built from placements, not
    from vehicles allocated to grid districts (Q12).

    Asserts that every vehicle type occurring in the pool has an entry in
    the `tech_data` block of `metadata_simbev_run.json` (D11).

    In test mode only the profiles of the imported charging events are
    imported.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        pool = pq.read_table(input_file(scenario_name, "ev_pool"))

        vehicle_profiles = (
            pq.read_table(
                input_file(scenario_name, "ev_mapping_ev_municipality"),
                columns=["ev_id"],
            )
            .column("ev_id")
            .to_numpy()
            .astype("int64")
        )
        pool_ev_ids = pool.column("ev_id").to_numpy().astype("int64")
        n_vehicles = np.bincount(
            vehicle_profiles,
            minlength=int(max(pool_ev_ids.max(), vehicle_profiles.max())) + 1,
        )
        unknown = np.setdiff1d(np.unique(vehicle_profiles), pool_ev_ids)
        if len(unknown):
            raise ValueError(
                f"{len(unknown)} ev_id values of the vehicles of scenario "
                f"'{scenario_name}' are not in ev_pool (sample: "
                f"{unknown[:10].tolist()})."
            )

        referenced = _referenced_ev_ids(scenario_name)
        if referenced is not None:
            pool = pool.filter(_is_in(pool.column("ev_id"), referenced))
            pool_ev_ids = pool.column("ev_id").to_numpy().astype("int64")

        types = sorted(set(pool.column("type").to_pylist()))
        tech_data = read_simbev_metadata_file(scenario_name, "tech_data")
        missing = [_ for _ in types if _ not in tech_data.index]
        if missing:
            raise AssertionError(
                f"Vehicle types {missing} occur in the EV pool of "
                f"scenario '{scenario_name}' but have no `tech_data` "
                f"entry in its metadata_simbev_run.json. Battery "
                f"capacity and energy consumption of those types are "
                f"unknown, so the model cannot be built."
            )

        unexpected = [_ for _ in types if _ not in EV_TYPES]
        if unexpected:
            print(
                f"  WARNING: pool contains vehicle types that are not "
                f"part of the documented taxonomy: {unexpected}."
            )

        print(
            f"  Importing {pool.num_rows} profiles of types {types} "
            f"({int(n_vehicles[pool_ev_ids].sum())} vehicles)..."
        )
        _clear_scenario(_qualified(EgonEvMitLgvPool), scenario_name)
        _copy_arrow_table(
            pa.table(
                {
                    "scenario": _constant_column(scenario_name, pool.num_rows),
                    "ev_id": pool.column("ev_id"),
                    "rs7_id": pool.column("rs7_id"),
                    "type": pool.column("type"),
                    "n_vehicles": pa.array(n_vehicles[pool_ev_ids]),
                }
            ),
            _qualified(EgonEvMitLgvPool),
            ["scenario", "ev_id", "rs7_id", "type", "n_vehicles"],
        )


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


def _log_geolis_counts(scenario_name: str) -> None:
    """Log the result counts stated by GeoLIS against the delivered files.

    The providers state them, so they are free assertions.
    """
    try:
        geolis = read_geolis_metadata_file(scenario_name)
    except FileNotFoundError as e:
        print(f"  {e}")
        return

    delivered = {
        "n_vehicle_instances": pq.ParquetFile(
            input_file(scenario_name, "ev_mapping_ev_municipality")
        ).metadata.num_rows,
        "n_distinct_evs": pq.ParquetFile(
            input_file(scenario_name, "ev_pool")
        ).metadata.num_rows,
        "n_event_rows": pq.ParquetFile(
            input_file(scenario_name, "ev_event")
        ).metadata.num_rows,
        "n_locations": pq.ParquetFile(
            input_file(scenario_name, "ev_charging_location")
        ).metadata.num_rows,
    }

    for key, value in delivered.items():
        stated = geolis.get(key)
        status = "OK" if stated == value else "MISMATCH"
        print(
            f"    GeoLIS {key}: stated {stated}, delivered {value} "
            f"[{status}]"
        )


def log_import_consistency() -> None:
    """Log consistency information about the imported tables.

    Non-fatal by design: what is fatal is decided by the dataset's
    validation rules.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        summary = db.select_dataframe(
            f"""
            SELECT
                (SELECT COUNT(*) FROM {_qualified(EgonEvMitLgvPool)}
                 WHERE scenario = '{scenario_name}') AS profiles,
                (SELECT COUNT(*) FROM {_qualified(EgonEvMitLgvTrip)}
                 WHERE scenario = '{scenario_name}') AS charging_events,
                (SELECT COUNT(*)
                 FROM {_qualified(EgonEvMitLgvChargingLocation)}
                 WHERE scenario = '{scenario_name}') AS locations,
                (SELECT COUNT(*)
                 FROM {_qualified(EgonEvMitLgvLocationEvents)}
                 WHERE scenario = '{scenario_name}') AS locations_used,
                (SELECT SUM(CARDINALITY(event_ids))
                 FROM {_qualified(EgonEvMitLgvLocationEvents)}
                 WHERE scenario = '{scenario_name}') AS placements,
                (SELECT SUM(p.n_vehicles)
                 FROM {_qualified(EgonEvMitLgvTrip)} t
                 JOIN {_qualified(EgonEvMitLgvPool)} p
                    ON p.scenario = t.scenario AND p.ev_id = t.ev_id
                 WHERE t.scenario = '{scenario_name}') AS expected
            """
        ).iloc[0]
        print(
            f"  profiles: {summary.profiles}, charging events: "
            f"{summary.charging_events}, locations: {summary.locations} "
            f"({summary.locations_used} with placements), placements: "
            f"{summary.placements}"
        )
        if testmode_off():
            status = (
                "OK" if summary.placements == summary.expected else "MISMATCH"
            )
            print(
                f"  placements expected from the vehicles per profile: "
                f"{summary.expected} [{status}]"
            )
        _log_geolis_counts(scenario_name)
