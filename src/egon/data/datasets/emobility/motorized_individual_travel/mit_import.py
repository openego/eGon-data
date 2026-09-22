"""
Import of the delivered M1+N1 input data (new methodology).

The new methodology no longer derives the electric vehicle fleet from
KBA registration statistics. One bundle per scenario is delivered as
parquet, downloaded from Zenodo by
:func:`download_and_extract`, imported into the database and finally
allocated to MV grid districts by
:func:`allocate_ev_instances_to_grid_districts`.

Call order::

    download_and_extract()

      * import_ev_pool()
      * import_ev_counts()
      * import_ev_municipality_mapping()
      * import_ev_events()

    allocate_ev_instances_to_grid_districts()

Notes
-----
`ev_mapping_event_location.parquet` -- the mapping of charging events to
charging locations -- is **not** imported. It carries one row per
(charging event x drawn vehicle), i.e. ~5e8 rows for `status2024` and an
estimated ~8e9 for `reGon2045`, which is roughly 440 GB in PostgreSQL
against a ~350 GB budget for the whole pipeline. Nothing in the database
needs it: charging locations carry their own `use_case`. Consumers read
the parquet file from the extracted archive directly.

Large files are read in row group chunks with pyarrow, filtered, and
streamed into PostgreSQL via ``COPY ... FROM STDIN``. This keeps the
throughput of a bulk import without multi-GB temporary CSV files and
lets test mode push the boundary filter into the read.
"""

from contextlib import contextmanager
import io

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
    EgonEvMitLgvCountMunicipality,
    EgonEvMitLgvCountMvGridDistrict,
    EgonEvMitLgvMappingEvMunicipality,
    EgonEvMitLgvMvGridDistrict,
    EgonEvMitLgvPool,
    EgonEvMitLgvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.ev_allocation import (  # noqa: E501
    mvgd_population_shares,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    EV_TYPES,
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


def selected_ev_ids(scenario_name: str):
    """Pool EV ids that are used inside the dataset boundary.

    Test mode filters the delivered municipality mapping to the AGS
    inside the boundary; the selection then cascades to the pool and the
    events, which is referentially consistent by construction.

    Read from the parquet file rather than from the database so that the
    import tasks stay independent of each other and can run in parallel.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    numpy.ndarray or None
        Sorted pool EV ids, or None when no filter is needed

    Notes
    -----
    Expect this to shrink the event table far less than the region share
    suggests. The pool is national and each profile is instantiated many
    times across Germany, so more than half of all profiles are used
    somewhere even in a single federal state.
    """
    ags = boundary_ags()
    if ags is None:
        return None

    mapping = pq.read_table(
        input_file(scenario_name, "ev_mapping_ev_municipality"),
        columns=["ev_id", "ags"],
        filters=[("ags", "in", ags.tolist())],
    )
    ev_ids = np.sort(pc.unique(mapping.column("ev_id")).to_numpy())

    print(
        f"  Test mode: {len(ags)} municipalities inside the boundary, "
        f"{mapping.num_rows} vehicle instances, "
        f"{len(ev_ids)} distinct pool EVs."
    )
    return ev_ids


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


def import_ev_pool() -> None:
    """Import `ev_pool.parquet` into `demand.egon_ev_mit_lgv_pool`.

    Asserts that every vehicle type occurring in the pool has an entry
    in the `tech_data` block of `metadata_simbev_run.json` (D11). A
    missing entry would otherwise surface as a `KeyError` per grid
    district deep inside the timeseries generation, once for every
    parallel task.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        ev_ids = selected_ev_ids(scenario_name)

        pool = pq.read_table(input_file(scenario_name, "ev_pool"))
        if ev_ids is not None:
            pool = pool.filter(_is_in(pool.column("ev_id"), ev_ids))

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

        print(f"  Importing {pool.num_rows} pool EVs of types {types}...")
        _clear_scenario(_qualified(EgonEvMitLgvPool), scenario_name)
        _copy_arrow_table(
            pa.table(
                {
                    "scenario": _constant_column(scenario_name, pool.num_rows),
                    "ev_id": pool.column("ev_id"),
                    "rs7_id": pool.column("rs7_id"),
                    "type": pool.column("type"),
                }
            ),
            _qualified(EgonEvMitLgvPool),
            ["scenario", "ev_id", "rs7_id", "type"],
        )


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

        delivered_ags = np.sort(delivered.column("ags").to_numpy())
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

        _log_ags_coverage(scenario_name, delivered_ags)
        _log_fleet_total(scenario_name, delivered)


def _log_ags_coverage(scenario_name: str, delivered_ags) -> None:
    """Log the two-way AGS difference against VG250.

    Both directions matter because the municipality-to-grid-district
    split joins on `ags`: an unmatched key on either side silently drops
    vehicles.
    """
    with db.session_scope() as session:
        query = session.query(func.distinct(Vg250Gem.ags).label("ags"))
    vg250_ags = set(
        pd.read_sql(query.statement, query.session.bind)
        .ags.astype("int64")
        .to_list()
    )

    delivered = set(int(_) for _ in delivered_ags)

    if not testmode_off():
        # VG250 holds only the municipalities inside the boundary, so
        # every delivered municipality outside it would be reported as
        # unknown. The two-way difference is only meaningful on a full
        # run.
        print(
            f"  Test mode: {len(delivered)} municipalities delivered "
            f"nationwide, {len(vg250_ags)} inside the boundary, "
            f"{len(delivered & vg250_ags)} of those delivered, "
            f"{len(vg250_ags - delivered)} inside the boundary without "
            f"delivered vehicles. The full two-way AGS difference is "
            f"only checked on a Germany-wide run."
        )
        return

    unknown = sorted(delivered - vg250_ags)
    without_vehicles = sorted(vg250_ags - delivered)

    print(
        f"  AGS coverage: {len(delivered)} municipalities delivered, "
        f"{len(vg250_ags)} in VG250."
    )
    print(
        f"    Delivered but unknown to VG250: {len(unknown)} "
        f"(sample: {unknown[:10]})"
    )
    print(
        f"    In VG250 but without delivered vehicles: "
        f"{len(without_vehicles)} (sample: {without_vehicles[:10]}) -- "
        f"harmless, those grid districts simply get none."
    )


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


def import_ev_municipality_mapping() -> None:
    """Import `ev_mapping_ev_municipality.parquet`.

    One row per vehicle: a pool EV occurring *n* times in the same `ags`
    yields *n* rows with distinct `id`. This is what makes the
    municipality-to-grid-district split an instance-level operation.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        ags = boundary_ags()

        source = input_file(scenario_name, "ev_mapping_ev_municipality")
        parquet_file = pq.ParquetFile(source)
        imported = 0

        _clear_scenario(
            _qualified(EgonEvMitLgvMappingEvMunicipality), scenario_name
        )
        with _copy_target(
            _qualified(EgonEvMitLgvMappingEvMunicipality),
            ["scenario", "id", "ev_id", "ags"],
        ) as write:
            for group in range(parquet_file.num_row_groups):
                mapping = parquet_file.read_row_group(group)
                if ags is not None:
                    mapping = mapping.filter(
                        _is_in(mapping.column("ags"), ags)
                    )
                imported += mapping.num_rows
                write(
                    pa.table(
                        {
                            "scenario": _constant_column(
                                scenario_name, mapping.num_rows
                            ),
                            "id": mapping.column("id"),
                            "ev_id": mapping.column("ev_id"),
                            "ags": mapping.column("ags"),
                        }
                    )
                )

        print(f"  Imported {imported} vehicle instances.")
        _log_geolis_counts(scenario_name)


def _log_geolis_counts(scenario_name: str) -> None:
    """Log the result counts stated by GeoLIS against what was imported.

    The providers state them, so they are free assertions. In test mode
    the imported counts are a subset by design and only the delivered
    file counts are checked.
    """
    try:
        geolis = read_geolis_metadata_file(scenario_name)
    except FileNotFoundError as e:
        print(f"  {e}")
        return

    stated = {
        "n_vehicle_instances": geolis.get("n_vehicle_instances"),
        "n_distinct_evs": geolis.get("n_distinct_evs"),
        "n_event_rows": geolis.get("n_event_rows"),
        "n_locations": geolis.get("n_locations"),
    }
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

    for key, value in stated.items():
        status = "OK" if value == delivered[key] else "MISMATCH"
        print(
            f"    GeoLIS {key}: stated {value}, delivered "
            f"{delivered[key]} [{status}]"
        )


def import_ev_events() -> None:
    """Import `ev_event.parquet` into `demand.egon_ev_mit_lgv_trip`.

    Read in row group chunks and streamed into PostgreSQL.
    `charging_use_case` is renamed to `use_case` on import (D12);
    `park_time_timesteps` is not imported, it is
    `park_end - park_start` and the model code ignores it.
    """
    columns = ["scenario"] + list(EVENT_COLUMN_MAPPING.values())

    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        ev_ids = selected_ev_ids(scenario_name)

        pool_ev_ids = set(
            pq.read_table(
                input_file(scenario_name, "ev_pool"), columns=["ev_id"]
            )
            .column("ev_id")
            .to_pylist()
        )

        source = input_file(scenario_name, "ev_event")
        parquet_file = pq.ParquetFile(source)
        imported = 0
        orphans = set()

        _clear_scenario(_qualified(EgonEvMitLgvTrip), scenario_name)
        with _copy_target(_qualified(EgonEvMitLgvTrip), columns) as write:
            for batch in parquet_file.iter_batches(
                batch_size=EVENT_CHUNK_SIZE,
                columns=list(EVENT_COLUMN_MAPPING.keys()),
            ):
                events = pa.Table.from_batches([batch])
                if ev_ids is not None:
                    events = events.filter(
                        _is_in(events.column("ev_id"), ev_ids)
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
                        }
                    )
                )
                print(f"    {imported} events imported...")

        print(f"  Imported {imported} events.")
        if orphans:
            print(
                f"  WARNING: {len(orphans)} ev_id values occur in "
                f"ev_event but not in ev_pool (sample: "
                f"{sorted(orphans)[:10]}). Their events have no vehicle "
                f"type and will not reach the model."
            )


def _largest_remainder_allocation(
    instance_counts: pd.DataFrame, shares: pd.DataFrame
) -> pd.DataFrame:
    """Distribute municipal vehicle counts over MV grid districts.

    Per (`ags`, `type`) the delivered instance count is distributed over
    the intersecting grid districts by population share, using largest
    remainder rounding. Municipal totals are preserved exactly and no
    random number generator is involved: remainder ties are broken by
    `bus_id`, so the result is reproducible.

    For the ~92 % of municipalities that lie inside a single grid
    district this degenerates to a plain copy.

    Parameters
    ----------
    instance_counts : pandas.DataFrame
        Columns `ags`, `type_code`, `count`
    shares : pandas.DataFrame
        Columns `ags`, `bus_id`, `share` (summing to 1 per `ags`)

    Returns
    -------
    pandas.DataFrame
        Columns `ags`, `type_code`, `bus_id`, `n`, sorted by
        (`ags`, `type_code`, `bus_id`)
    """
    allocation = instance_counts.merge(shares, on="ags", how="inner")
    allocation = allocation.sort_values(
        ["ags", "type_code", "bus_id"], kind="stable"
    ).reset_index(drop=True)

    exact = allocation["count"] * allocation["share"]
    allocation["base"] = np.floor(exact).astype("int64")
    allocation["remainder"] = exact - allocation["base"]

    groups = allocation.groupby(["ags", "type_code"], sort=False)
    leftover = groups["count"].transform("first") - groups["base"].transform(
        "sum"
    )
    # `method="first"` breaks remainder ties by row order, which is
    # bus_id order thanks to the sort above.
    remainder_rank = groups["remainder"].rank(method="first", ascending=False)
    allocation["n"] = allocation["base"] + (remainder_rank <= leftover).astype(
        "int64"
    )

    return allocation[["ags", "type_code", "bus_id", "n"]]


def allocate_ev_instances_to_grid_districts() -> None:
    """Place the delivered vehicle instances in MV grid districts.

    Writes `demand.egon_ev_mit_lgv_mv_grid_district` -- one row per
    vehicle instance, carrying the municipality it came from -- and
    aggregates `demand.egon_ev_mit_lgv_count_mv_grid_district`.

    Vehicles in municipalities that VG250 does not know are dropped and
    the loss is quantified in the log: an AGS vintage mismatch between
    VG250 and the delivery is an ordinary occurrence -- municipalities
    merge and are renumbered -- and aborting the pipeline on reference
    data drift is a worse outcome than proceeding with a documented,
    quantified loss. If the loss turns out material, the fix is an AGS
    translation table, not a tolerance.
    """
    scenarios = new_scenarios()
    if not scenarios:
        print(
            "No scenario on the new methodology configured, nothing to "
            "allocate."
        )
        return

    print("Loading population shares of municipalities in grid districts...")
    population_shares = mvgd_population_shares()
    shares = population_shares[
        ["ags", "bus_id", "pop_mun_in_mvgd", "pop_mun_in_mvgd_of_mun_total"]
    ].copy()
    shares["ags"] = shares.ags.astype("int64")
    shares["bus_id"] = shares.bus_id.astype("int64")

    # Normalise so the shares of one municipality sum to exactly 1 --
    # `pop_mun_in_mvgd_of_mun_total` is computed against the municipal
    # total and can miss a fraction of a percent to rounding. A
    # municipality without population in any of its grid districts would
    # divide by zero; its vehicles are spread evenly instead, which is
    # the only defensible split without a weight.
    total = shares.groupby("ags")["pop_mun_in_mvgd_of_mun_total"].transform(
        "sum"
    )
    unweighted = total <= 0
    shares["share"] = shares.pop_mun_in_mvgd_of_mun_total / total
    if unweighted.any():
        parts = shares.groupby("ags")["bus_id"].transform("size")
        shares.loc[unweighted, "share"] = 1 / parts[unweighted]
        print(
            f"  {shares.loc[unweighted, 'ags'].nunique()} municipalities "
            f"have no population in any of their grid districts; their "
            f"vehicles are spread evenly."
        )

    for scenario_name in scenarios:
        print(f"SCENARIO: {scenario_name}")
        _allocate_scenario(scenario_name, shares)


def _allocate_scenario(scenario_name: str, shares: pd.DataFrame) -> None:
    """Place one scenario's vehicle instances in MV grid districts."""
    ags_filter = boundary_ags()
    variation = scenario_variation(scenario_name)
    type_codes = {name: code for code, name in enumerate(EV_TYPES)}

    print("  Loading the delivered vehicle instances...")
    mapping = pq.read_table(
        input_file(scenario_name, "ev_mapping_ev_municipality"),
        columns=["id", "ev_id", "ags"],
        filters=(
            None
            if ags_filter is None
            else [("ags", "in", ags_filter.tolist())]
        ),
    )
    pool = pq.read_table(
        input_file(scenario_name, "ev_pool"), columns=["ev_id", "type"]
    ).to_pandas()

    ev_type_code = pd.Series(
        pool.type.map(type_codes).to_numpy(), index=pool.ev_id.to_numpy()
    )
    unknown_types = pool.type[~pool.type.isin(type_codes)].unique()
    if len(unknown_types) > 0:
        raise ValueError(
            f"Pool of scenario '{scenario_name}' contains vehicle types "
            f"outside the documented taxonomy: {list(unknown_types)}. "
            f"Add them to `EV_TYPES` and to the count tables first."
        )

    ids = mapping.column("id").to_numpy()
    ev_ids = mapping.column("ev_id").to_numpy()
    ags = mapping.column("ags").to_numpy().astype("int64")
    codes = ev_type_code.reindex(ev_ids).to_numpy(dtype="float64")
    if np.isnan(codes).any():
        missing = np.unique(ev_ids[np.isnan(codes)])
        raise ValueError(
            f"{len(missing)} ev_id values of the municipality mapping "
            f"of scenario '{scenario_name}' are not in the EV pool "
            f"(sample: {missing[:10].tolist()})."
        )
    codes = codes.astype("int64")

    # One integer key per (ags, type) so the whole instance-level
    # operation stays in numpy: `len(EV_TYPES) <= 16`.
    keys = ags * 16 + codes
    order = np.lexsort((ids, keys))
    keys_sorted = keys[order]
    unique_keys, counts = np.unique(keys_sorted, return_counts=True)

    instance_counts = pd.DataFrame(
        {
            "ags": unique_keys // 16,
            "type_code": unique_keys % 16,
            "count": counts,
        }
    )

    _log_vehicle_loss(scenario_name, instance_counts, shares, type_codes)

    allocation = _largest_remainder_allocation(instance_counts, shares)

    # Both sides are sorted by (ags, type_code) and their per-group
    # totals agree, so repeating the grid district of each allocation
    # row aligns it with the instances in `id` order.
    placed = allocation[allocation.n > 0]
    kept_keys = placed.ags.to_numpy() * 16 + placed.type_code.to_numpy()
    kept_instances = np.isin(keys_sorted, np.unique(kept_keys))

    bus_ids = np.repeat(placed.bus_id.to_numpy(), placed.n.to_numpy())
    if bus_ids.size != kept_instances.sum():
        raise AssertionError(
            f"Allocation of scenario '{scenario_name}' produced "
            f"{bus_ids.size} placements for {int(kept_instances.sum())} "
            f"vehicle instances. The largest remainder split must "
            f"preserve municipal totals exactly."
        )

    print(f"  Writing {bus_ids.size} vehicle instances to grid districts...")
    _write_mv_grid_district(
        scenario_name=scenario_name,
        variation=variation,
        bus_ids=bus_ids,
        ev_ids=ev_ids[order][kept_instances],
        ags=ags[order][kept_instances],
    )
    _write_grid_district_counts(
        scenario_name=scenario_name,
        variation=variation,
        allocation=placed,
        shares=shares,
    )


def _log_vehicle_loss(
    scenario_name: str,
    instance_counts: pd.DataFrame,
    shares: pd.DataFrame,
    type_codes: dict,
) -> None:
    """Quantify the vehicles lost to municipalities unknown to VG250."""
    known = set(shares.ags.unique())
    lost = instance_counts[~instance_counts.ags.isin(known)]
    total = int(instance_counts["count"].sum())

    if lost.empty:
        print(
            f"  All {total} delivered vehicle instances are in "
            f"municipalities known to VG250."
        )
        return

    names = {code: name for name, code in type_codes.items()}
    per_type = (
        lost.groupby("type_code")["count"]
        .sum()
        .rename(index=names)
        .sort_values(ascending=False)
    )
    lost_ags = sorted(lost.ags.unique())
    lost_total = int(lost["count"].sum())

    print(
        f"  WARNING: {len(lost_ags)} delivered municipalities are "
        f"unknown to VG250 and their vehicles are dropped "
        f"(sample: {lost_ags[:10]})."
    )
    print(f"    Vehicles lost per type: {per_type.to_dict()}")
    print(
        f"    Vehicles lost in total: {lost_total} of {total} "
        f"({lost_total / total:.3%} of the delivered fleet of "
        f"scenario '{scenario_name}')."
    )
    if total and lost_total / total > 0.005:
        print(
            "    This exceeds the ~0.5 % rule of thumb. Dropping is "
            "then the wrong answer: an AGS vintage translation table "
            "(delivered ags -> current VG250 ags) is needed."
        )


def _write_mv_grid_district(
    scenario_name: str, variation: str, bus_ids, ev_ids, ags
) -> None:
    """Write the vehicle instances of one scenario, chunked."""
    table = _qualified(EgonEvMitLgvMvGridDistrict)
    columns = ["scenario", "scenario_variation", "bus_id", "ev_id", "ags"]
    chunk = 2_000_000

    _clear_scenario(table, scenario_name)
    with _copy_target(table, columns) as write:
        for start in range(0, bus_ids.size, chunk):
            stop = min(start + chunk, bus_ids.size)
            length = stop - start
            write(
                pa.table(
                    {
                        "scenario": _constant_column(scenario_name, length),
                        "scenario_variation": _constant_column(
                            variation, length
                        ),
                        "bus_id": pa.array(bus_ids[start:stop]),
                        "ev_id": pa.array(ev_ids[start:stop]),
                        "ags": pa.array(ags[start:stop]),
                    }
                )
            )
            print(f"    {stop} of {bus_ids.size} instances written...")


def _write_grid_district_counts(
    scenario_name: str,
    variation: str,
    allocation: pd.DataFrame,
    shares: pd.DataFrame,
) -> None:
    """Aggregate and write the vehicle counts per MV grid district."""
    names = {code: name for code, name in enumerate(EV_TYPES)}
    counts = (
        allocation.assign(type=allocation.type_code.map(names))
        .pivot_table(
            index="bus_id",
            columns="type",
            values="n",
            aggfunc="sum",
            fill_value=0,
        )
        .reindex(columns=list(EV_TYPES), fill_value=0)
        .astype("int64")
        .reset_index()
    )

    # RegioStaR7 id of a grid district: taken from the municipality with
    # the highest population share in it, as in the legacy allocation.
    rs7 = db.select_dataframe(
        f"""
        SELECT ags, rs7_id
        FROM demand.egon_ev_mit_lgv_count_municipality
        WHERE scenario = '{scenario_name}'
        """
    ).astype({"ags": "int64"})
    dominant = (
        shares.merge(rs7, on="ags", how="inner")
        .sort_values(["bus_id", "pop_mun_in_mvgd"], ascending=[True, False])
        .drop_duplicates("bus_id", keep="first")[["bus_id", "rs7_id"]]
    )

    counts = counts.merge(dominant, on="bus_id", how="left")
    # Nullable so a grid district without a RegioStaR7 reference stays
    # NULL rather than turning the column into floats.
    counts["rs7_id"] = counts.rs7_id.astype("Int64")
    counts["scenario"] = scenario_name
    counts["scenario_variation"] = variation

    print(
        f"  Writing vehicle counts for {len(counts)} grid districts "
        f"({int(counts[list(EV_TYPES)].sum().sum())} vehicles)."
    )
    _clear_scenario(_qualified(EgonEvMitLgvCountMvGridDistrict), scenario_name)
    counts.to_sql(
        name=EgonEvMitLgvCountMvGridDistrict.__table__.name,
        schema=EgonEvMitLgvCountMvGridDistrict.__table__.schema,
        con=db.engine(),
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    municipal_total = db.select_dataframe(
        f"""
        SELECT {' + '.join(f'COALESCE(SUM({_}), 0)' for _ in EV_TYPES)}
                   AS total
        FROM demand.egon_ev_mit_lgv_count_municipality
        WHERE scenario = '{scenario_name}'
        """
    ).total.iloc[0]
    grid_total = int(counts[list(EV_TYPES)].sum().sum())
    print(
        f"    Municipal total {int(municipal_total)} vs. grid district "
        f"total {grid_total} "
        f"({'match' if int(municipal_total) == grid_total else 'MISMATCH'}"
        f" -- a difference is the vehicle loss logged above)."
    )


def log_import_consistency() -> None:
    """Log consistency information about the imported tables.

    Non-fatal by design: the choice of what is fatal stays with the
    validation concept that will replace the obsolete sanity checks.
    """
    for scenario_name in new_scenarios():
        print(f"SCENARIO: {scenario_name}")
        summary = db.select_dataframe(
            f"""
            SELECT
                (SELECT COUNT(*) FROM demand.egon_ev_mit_lgv_pool
                 WHERE scenario = '{scenario_name}') AS pool,
                (SELECT COUNT(*) FROM demand.egon_ev_mit_lgv_trip
                 WHERE scenario = '{scenario_name}') AS events,
                (SELECT COUNT(*)
                 FROM demand.egon_ev_mit_lgv_mapping_ev_municipality
                 WHERE scenario = '{scenario_name}') AS instances,
                (SELECT COUNT(*)
                 FROM demand.egon_ev_mit_lgv_mv_grid_district
                 WHERE scenario = '{scenario_name}') AS placed,
                (SELECT COUNT(*)
                 FROM demand.egon_ev_mit_lgv_count_municipality
                 WHERE scenario = '{scenario_name}') AS municipalities
            """
        ).iloc[0]
        print(
            f"  pool: {summary.pool}, events: {summary.events}, "
            f"delivered instances: {summary.instances}, "
            f"placed instances: {summary.placed}, "
            f"municipalities: {summary.municipalities}"
        )

        orphans = db.select_dataframe(
            f"""
            SELECT COUNT(*) AS n FROM (
                SELECT DISTINCT m.ev_id
                FROM demand.egon_ev_mit_lgv_mapping_ev_municipality m
                LEFT JOIN demand.egon_ev_mit_lgv_pool p
                    ON p.ev_id = m.ev_id
                    AND p.scenario = m.scenario
                WHERE m.scenario = '{scenario_name}'
                    AND p.ev_id IS NULL
            ) o
            """
        ).n.iloc[0]
        print(
            f"  ev_id values of the municipality mapping missing from "
            f"the pool: {int(orphans)}"
        )
