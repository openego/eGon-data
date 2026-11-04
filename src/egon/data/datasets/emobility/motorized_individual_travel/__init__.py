"""
Main module for preparation of model data (static and timeseries) for
motorized individual travel (MIT) and light commercial vehicles (LGV).

**Contents of this module**
  * Creation of DB tables
  * Download and import of the delivered M1+N1 input data
  * Download and preprocessing of vehicle registration data from KBA and
    BMVI (legacy methodology)
  * Calculate number of electric vehicles and allocate on different spatial
    levels.
  * Extract and write pre-generated trips to DB

Notes
-----
Two methodologies live side by side, dispatched per scenario by
:func:`egon.data.datasets.emobility.mit_lgv_input_data.is_legacy_scenario`:

* the **new** one (`status2024`, `reGon2037`, `reGon2045`) imports one
  delivered bundle per scenario -- vehicle profiles, their charging
  events, charging locations and the placements of every vehicle's
  charging events at those locations. It covers vehicle class M1
  (passenger cars) *and* N1 (light commercial vehicles < 3.5 t). The
  eTraGo model is built from the **placements**: one district model per
  MV grid district from its charging locations at voltage levels 4-7, and
  one transmission model per grid connection at levels 1-3. See
  :mod:`.charging_locations`, :mod:`.mit_import` and
  :mod:`.placement_model`.
* the **legacy** one (`eGon2035`) keeps the simBEV trip tarball from the
  data bundle, derives the fleet from KBA registration statistics and
  builds one model per MV grid district from the vehicles allocated to it
  (:mod:`.model_timeseries`). It is kept as a long term stable scenario
  and is not in the defaults.

Both write the same tables, discriminated by `scenario`. Note that they
have to be built in the *same* run: :func:`create_tables` drops and
recreates all tables, so running the legacy scenario afterwards would
destroy the new scenarios' rows.
"""

from pathlib import Path
from urllib.request import urlretrieve
import json
import os
import tarfile

from airflow.operators.python import PythonOperator
from egon_validation import (
    DataTypeValidation,
    NotNullAndNotNaNValidation,
    ValueSetValidation,
)
from psycopg2.extensions import AsIs, register_adapter
import numpy as np
import pandas as pd

from egon.data import config, db, subprocess
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.emobility.mit_lgv_input_data import (
    INPUT_FILES,
    ZENODO_URLS,
)
from egon.data.datasets.emobility.motorized_individual_travel import (
    flex_diagnostics,
)
from egon.data.datasets.emobility.motorized_individual_travel.charging_locations import (  # noqa: E501
    import_charging_locations,
)
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvMitLgvChargingLocation,
    EgonEvMitLgvCountMunicipality,
    EgonEvMitLgvCountMvGridDistrict,
    EgonEvMitLgvCountRegistrationDistrict,
    EgonEvMitLgvLocationEvents,
    EgonEvMitLgvMetadata,
    EgonEvMitLgvMvGridDistrict,
    EgonEvMitLgvPool,
    EgonEvMitLgvTrip,
    add_metadata,
)
from egon.data.datasets.emobility.motorized_individual_travel.ev_allocation import (  # noqa: E501
    allocate_evs_numbers,
    allocate_evs_to_grid_districts,
)
from egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics import (  # noqa: E501
    log_diagnostics_checks,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    CHARGING_USE_CASES,
    COLUMNS_KBA,
    DATA_BUNDLE_DIR,
    EV_TYPES,
    MODEL_TYPES,
    TRIP_COLUMN_MAPPING,
    WORKING_DIR,
    is_legacy_scenario,
    legacy_scenarios,
    read_geolis_metadata_file,
    simbev_metadata_path,
)
from egon.data.datasets.emobility.motorized_individual_travel.mit_import import (  # noqa: E501
    download_and_extract,
    import_charging_events,
    import_ev_counts,
    import_ev_pool,
    import_location_events,
    log_import_consistency,
)
from egon.data.datasets.emobility.motorized_individual_travel.model_timeseries import (  # noqa: E501
    delete_model_data_from_db,
    generate_model_data_bunch,
    generate_model_data_remaining,
)
from egon.data.datasets.emobility.motorized_individual_travel.placement_model import (  # noqa: E501
    generate_placement_models,
)
from egon.data.validation import TableValidation
from egon.data.validation.rules.custom.sanity import (
    MitBandReachable,
    MitBusByLevel,
    MitDeficitWithinBattery,
    MitDiagnostics,
    MitEventArrayReferences,
    MitEventArrayTotal,
    MitEventArrayUseCase,
    MitFallbackDistance,
    MitImportCounts,
    MitLinkParams,
    MitLocationAssigned,
    MitLowflexPair,
    MitModelComponents,
    MitModelEnergy,
    MitOrphans,
    MitScenarioCoverage,
    MitStoreBand,
    MitTechData,
    MitTotalEnergy,
    MitTransmissionModels,
    MitVoltageLevel,
)

#: Scenarios on the new methodology, the ones the validation rules check.
NEW_SCENARIOS = tuple(ZENODO_URLS)


# ========== Register np datatypes with SQLA ==========
def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.int64, adapt_numpy_int64)
# =====================================================


def create_tables():
    """Create tables for electric vehicles and light commercial vehicles

    All tables are dropped and recreated, so every configured scenario
    has to be built in the same run.

    Returns
    -------
    None
    """

    engine = db.engine()
    for table in (
        EgonEvMitLgvCountRegistrationDistrict,
        EgonEvMitLgvCountMunicipality,
        EgonEvMitLgvCountMvGridDistrict,
        EgonEvMitLgvPool,
        EgonEvMitLgvTrip,
        EgonEvMitLgvMvGridDistrict,
        EgonEvMitLgvChargingLocation,
        EgonEvMitLgvLocationEvents,
        EgonEvMitLgvMetadata,
    ):
        table.__table__.drop(bind=engine, checkfirst=True)
        table.__table__.create(bind=engine, checkfirst=True)

    # Placements are stored as int arrays of several GB per scenario;
    # lz4 compresses them better and faster than the default pglz.
    db.execute_sql(
        f"ALTER TABLE {EgonEvMitLgvLocationEvents.__table__.schema}."
        f"{EgonEvMitLgvLocationEvents.__table__.name} "
        f"ALTER COLUMN event_ids SET COMPRESSION lz4;"
    )

    # The vehicle -> municipality mapping of the previous version of the
    # new methodology is no longer written; remove a left-over table.
    db.execute_sql(
        "DROP TABLE IF EXISTS demand.egon_ev_mit_lgv_mapping_ev_municipality;"
    )

    flex_diagnostics.create_tables()

    # Create dir for results, if it does not exist
    result_dir = WORKING_DIR / Path("results")
    result_dir.mkdir(exist_ok=True, parents=True)


def download_and_preprocess():
    """Downloads and preprocesses data from KBA and BMVI

    Returns
    -------
    pandas.DataFrame
        Vehicle registration data for registration district
    pandas.DataFrame
        RegioStaR7 data
    """
    if not legacy_scenarios(config.settings()["egon-data"]["--scenarios"]):
        print(
            "No scenario on the legacy methodology configured, skipping "
            "the KBA and RegioStaR7 download."
        )
        return

    mit_sources = MotorizedIndividualTravel.sources.files["original_data"][
        "original_data"
    ]["sources"]

    # Create the folder, if it does not exist
    WORKING_DIR.mkdir(parents=True, exist_ok=True)

    ################################
    # Download and import KBA data #
    ################################
    url = mit_sources["KBA"]["url"]
    file = WORKING_DIR / mit_sources["KBA"]["file"]
    if not os.path.isfile(file):
        urlretrieve(url, file)

    kba_data = pd.read_excel(
        file,
        sheet_name=mit_sources["KBA"]["sheet"],
        usecols=mit_sources["KBA"]["columns"],
        skiprows=mit_sources["KBA"]["skiprows"],
    )
    kba_data.columns = COLUMNS_KBA
    kba_data.replace(
        " ",
        np.nan,
        inplace=True,
    )
    kba_data = kba_data.dropna()
    kba_data[
        ["ags_reg_district", "reg_district"]
    ] = kba_data.reg_district.str.split(
        pat=" ",
        n=1,
        expand=True,
    )
    kba_data.ags_reg_district = kba_data.ags_reg_district.astype("int")

    kba_data.to_csv(
        WORKING_DIR / mit_sources["KBA"]["file_processed"], index=None
    )

    #######################################
    # Download and import RegioStaR7 data #
    #######################################

    url = mit_sources["RS7"]["url"]
    file = WORKING_DIR / mit_sources["RS7"]["file"]
    if not os.path.isfile(file):
        urlretrieve(url, file)

    rs7_data = pd.read_excel(file, sheet_name=mit_sources["RS7"]["sheet"])

    rs7_data["ags_district"] = (
        rs7_data.gem_20.multiply(1 / 1000).apply(np.floor).astype("int")
    )
    rs7_data = rs7_data.rename(
        columns={"gem_20": "ags", "RegioStaR7": "rs7_id"}
    )
    rs7_data.rs7_id = rs7_data.rs7_id.astype("int")

    rs7_data.to_csv(
        WORKING_DIR / mit_sources["RS7"]["file_processed"], index=None
    )


def extract_trip_file():
    """Extract trip file from data bundle

    Legacy methodology only: scenarios on the new methodology take their
    events from the delivered Zenodo archive, cf.
    :func:`egon.data.datasets.emobility.motorized_individual_travel.mit_import.download_and_extract`.
    """
    trip_dir = DATA_BUNDLE_DIR / Path("mit_trip_data")

    mit_sources = MotorizedIndividualTravel.sources.files["original_data"][
        "original_data"
    ]["sources"]

    for scenario_name in legacy_scenarios(
        config.settings()["egon-data"]["--scenarios"]
    ):
        print(f"SCENARIO: {scenario_name}")
        trip_file = trip_dir / Path(
            mit_sources["trips"][scenario_name]["file"]
        )

        tar = tarfile.open(trip_file)
        if os.path.isfile(trip_file):
            tar.extractall(trip_dir)
        else:
            raise FileNotFoundError(
                f"Trip file {trip_file} not found in data bundle."
            )


def write_evs_trips_to_db():
    """Write EVs and trips generated by simBEV from data bundle to database
    table

    Legacy methodology only.
    """
    mit_sources = MotorizedIndividualTravel.sources.files["original_data"][
        "original_data"
    ]["sources"]

    # Calculate TESTMODE_OFF locally using config.settings()
    testmode_off = (
        config.settings()["egon-data"]["--dataset-boundary"] == "Everything"
    )

    def import_csv(f):
        df = pd.read_csv(f, usecols=TRIP_COLUMN_MAPPING.keys())
        df["rs7_id"] = int(f.parent.name)
        df["simbev_ev_id"] = "_".join(f.name.split("_")[0:3])
        return df

    for scenario_name in legacy_scenarios(
        config.settings()["egon-data"]["--scenarios"]
    ):
        print(f"SCENARIO: {scenario_name}")
        trip_dir_name = Path(
            mit_sources["trips"][scenario_name]["file"].split(".")[0]
        )

        trip_dir_root = DATA_BUNDLE_DIR / Path("mit_trip_data", trip_dir_name)

        if testmode_off:
            trip_files = list(trip_dir_root.glob("*/*.csv"))
        else:
            # Load only 1000 EVs per region if in test mode
            trip_files = [
                list(rdir.glob("*.csv"))[:1000]
                for rdir in [_ for _ in trip_dir_root.iterdir() if _.is_dir()]
            ]
            # Flatten
            trip_files = [i for sub in trip_files for i in sub]

        # Read, concat and reorder cols
        print(f"Importing {len(trip_files)} EV trip CSV files...")
        trip_data = pd.concat(map(import_csv, trip_files))
        trip_data.rename(columns=TRIP_COLUMN_MAPPING, inplace=True)
        trip_data = trip_data.reset_index().rename(
            columns={"index": "simbev_event_id"}
        )
        cols = ["rs7_id", "simbev_ev_id", "simbev_event_id"] + list(
            TRIP_COLUMN_MAPPING.values()
        )
        trip_data.index.name = "event_id"
        trip_data = trip_data[cols]

        # Extract EVs from trips
        evs_unique = trip_data[["rs7_id", "simbev_ev_id"]].drop_duplicates()
        evs_unique = evs_unique.reset_index().drop(columns=["event_id"])
        evs_unique.index.name = "ev_id"

        # Add EV id to trip DF
        trip_data["ev_id"] = pd.merge(
            trip_data, evs_unique.reset_index(), on=["rs7_id", "simbev_ev_id"]
        )["ev_id"]

        # Split simBEV id into type and id
        evs_unique[["type", "simbev_ev_id"]] = evs_unique[
            "simbev_ev_id"
        ].str.rsplit(pat="_", n=1, expand=True)
        evs_unique.simbev_ev_id = evs_unique.simbev_ev_id.astype(int)
        evs_unique["scenario"] = scenario_name

        trip_data.drop(columns=["rs7_id", "simbev_ev_id"], inplace=True)
        trip_data["scenario"] = scenario_name
        trip_data.sort_index(inplace=True)

        # Write EVs to DB
        print("Writing EVs to DB pool...")
        evs_unique.to_sql(
            name=EgonEvMitLgvPool.__table__.name,
            schema=EgonEvMitLgvPool.__table__.schema,
            con=db.engine(),
            if_exists="append",
            index=True,
        )

        # Write trips to CSV and import to DB
        print("Writing EV trips to CSV file...")
        trip_file = WORKING_DIR / f"trip_data_{scenario_name}.csv"
        trip_data.to_csv(trip_file)

        # Get DB config
        trip_table = (
            f"{EgonEvMitLgvTrip.__table__.schema}."
            f"{EgonEvMitLgvTrip.__table__.name}"
        )
        docker_db_config = db.credentials()
        host = ["-h", f"{docker_db_config['HOST']}"]
        port = ["-p", f"{docker_db_config['PORT']}"]
        pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
        user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
        command = [
            "-c",
            rf"\copy {trip_table}"
            rf"({','.join(trip_data.reset_index().columns)})"
            rf" FROM '{str(trip_file)}' DELIMITER ',' CSV HEADER;",
        ]

        print("Importing EV trips from CSV file to DB...")
        subprocess.run(
            ["psql"] + host + port + pgdb + user + command,
            env={"PGPASSWORD": docker_db_config["POSTGRES_PASSWORD"]},
        )

        os.remove(trip_file)


def write_metadata_to_db():
    """Write the input data run configurations per scenario to database.

    Both run configurations are stored **whole**, as JSONB. The
    delivered `config.basic` is not a stable set of keys -- between the
    old bundle and delivery v1.4 it lost `grid_timeseries` and
    `grid_timeseries_by_usecase` and gained a dozen others -- so
    mirroring individual keys as columns kept breaking on every config
    change. Storing the document whole ends that class of failure and
    preserves the technical data and the GeoLIS result counts.

    `geolis_config` is NULL for scenarios on the legacy methodology,
    which ship no GeoLIS run.

    Notes
    -----
    egon-data's own model code is unaffected by the shape of this table:
    :func:`egon.data.datasets.emobility.motorized_individual_travel.helpers.read_simbev_metadata_file`
    reads the *file*, not the table.
    """
    rows = []

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
        print(f"SCENARIO: {scenario_name}")
        with open(simbev_metadata_path(scenario_name)) as f:
            simbev_config = json.load(f)

        if is_legacy_scenario(scenario_name):
            geolis_config = None
        else:
            geolis_config = read_geolis_metadata_file(scenario_name)

        rows.append(
            EgonEvMitLgvMetadata(
                scenario=scenario_name,
                simbev_config=simbev_config,
                geolis_config=geolis_config,
            )
        )

    with db.session_scope() as session:
        # Delete first so a task retry does not hit the primary key.
        session.query(EgonEvMitLgvMetadata).filter(
            EgonEvMitLgvMetadata.scenario.in_([_.scenario for _ in rows])
        ).delete(synchronize_session=False)
        session.add_all(rows)


def mit_validation_rules():
    """Validation rules for the tables written for the new scenarios.

    Content checks run per scenario of :data:`NEW_SCENARIOS`, one rule
    instance per scenario; eGon2035 is not checked. See
    :mod:`egon.data.validation.rules.custom.sanity.mit_charging`.

    Returns
    -------
    list
        TableValidation specs and rule instances.
    """
    tables = MotorizedIndividualTravel.targets.tables
    locations = tables["ev_charging_location"]
    location_events = tables["ev_location_events"]
    pool = tables["ev_pool"]
    pool_suffix = pool.split(".")[-1]

    # The pool's formal rules are listed one by one: simbev_ev_id is NULL
    # for the new scenarios and n_vehicles for eGon2035 by design, which
    # TableValidation's whole-table NOT NULL check would report.
    rules = [
        TableValidation(
            table_name=locations,
            geometry_columns=["geom"],
            data_type_columns={
                "location_id": "bigint",
                "scenario": "character varying",
                "charging_points": "integer",
                "average_charging_capacity": "integer",
                "use_case": "character varying",
                "candidate_uid": "character varying",
                "is_synthetic_location": "boolean",
                "mv_grid_id": "integer",
                "grid_connection_id": "integer",
                "voltage_level": "smallint",
                "model_type": "character varying",
                "bus_id": "integer",
                "assignment": "character varying",
                "geom": "geometry",
            },
            not_null_columns=[
                "location_id",
                "scenario",
                "charging_points",
                "use_case",
                "mv_grid_id",
                "grid_connection_id",
                "voltage_level",
                "model_type",
                "bus_id",
                "assignment",
            ],
            value_set_columns={
                "scenario": list(NEW_SCENARIOS),
                "use_case": list(CHARGING_USE_CASES),
                "model_type": list(MODEL_TYPES),
                "assignment": ["within", "nearest"],
            },
        ),
        TableValidation(
            table_name=location_events,
            data_type_columns={
                "scenario": "character varying",
                "location_id": "bigint",
                "event_ids": "ARRAY",
            },
            not_null_columns=["scenario", "location_id", "event_ids"],
            value_set_columns={"scenario": list(NEW_SCENARIOS)},
        ),
        DataTypeValidation(
            table=pool,
            rule_id=f"DATA_TYPES.{pool_suffix}",
            column_types={
                "scenario": "character varying",
                "ev_id": "integer",
                "rs7_id": "smallint",
                "type": "character varying",
                "simbev_ev_id": "integer",
                "n_vehicles": "integer",
            },
        ),
        NotNullAndNotNaNValidation(
            table=pool,
            rule_id=f"NOT_NAN.{pool_suffix}",
            columns=["scenario", "ev_id", "type"],
        ),
        ValueSetValidation(
            table=pool,
            rule_id=f"VALUE_SET_TYPE.{pool_suffix}",
            column="type",
            expected_values=list(EV_TYPES),
        ),
        MitScenarioCoverage(
            table="grid.egon_etrago_load",
            rule_id="SANITY_MIT_SCENARIO_COVERAGE",
        ),
        MitOrphans(
            table="grid.egon_etrago_link",
            rule_id="SANITY_MIT_ORPHANS",
        ),
    ]

    per_scenario = [
        (MitImportCounts, locations, "IMPORT_COUNTS"),
        (MitEventArrayTotal, location_events, "EVENT_ARRAY_TOTAL"),
        (MitEventArrayReferences, location_events, "EVENT_ARRAY_REFERENCES"),
        (MitEventArrayUseCase, location_events, "EVENT_ARRAY_USE_CASE"),
        (MitTechData, pool, "TECH_DATA"),
        (MitLocationAssigned, locations, "LOCATION_ASSIGNED"),
        (MitFallbackDistance, locations, "FALLBACK_DISTANCE"),
        (MitVoltageLevel, locations, "VOLTAGE_LEVEL"),
        (MitBusByLevel, locations, "BUS_BY_LEVEL"),
        (
            MitTransmissionModels,
            "grid.egon_etrago_link",
            "TRANSMISSION_MODELS",
        ),
        (MitModelEnergy, "grid.egon_etrago_load", "MODEL_ENERGY"),
        (MitTotalEnergy, "grid.egon_etrago_load", "TOTAL_ENERGY"),
        (MitModelComponents, "grid.egon_etrago_bus", "MODEL_COMPONENTS"),
        (MitStoreBand, "grid.egon_etrago_store", "STORE_BAND"),
        (
            MitDeficitWithinBattery,
            tables["ev_trip"],
            "DEFICIT_WITHIN_BATTERY",
        ),
        (MitLinkParams, "grid.egon_etrago_link", "LINK_PARAMS"),
        (MitBandReachable, "grid.egon_etrago_store", "BAND_REACHABLE"),
        (MitLowflexPair, "grid.egon_etrago_load", "LOWFLEX_PAIR"),
        (MitDiagnostics, tables["ev_flex_timeseries"], "DIAGNOSTICS"),
    ]
    for scenario in NEW_SCENARIOS:
        for rule_class, table, check in per_scenario:
            rules.append(
                rule_class(
                    table=table,
                    rule_id=f"SANITY_MIT_{check}.{scenario}",
                    scenario=scenario,
                )
            )
    return rules


class MotorizedIndividualTravel(Dataset):
    """
    Class to set up static and timeseries data for motorized individual travel
    (MIT, vehicle class M1) and light commercial vehicles (LGV, vehicle class
    N1).

    For more information see data documentation on :ref:`mobility-demand-mit-ref`.

    *Dependencies*
      * :py:class:`SubstationVoronoi
        <egon.data.datasets.substation_voronoi.SubstationVoronoi>`
      * :py:class:`Osmtgmod <egon.data.datasets.osmtgmod.Osmtgmod>`
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`MvGridDistricts
        <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`
      * :py:class:`ScenarioParameters
        <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`ZensusMvGridDistricts
        <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`
      * :py:class:`ZensusVg250 <egon.data.datasets.zensus_vg250.ZensusVg250>`
      * :py:class:`StorageEtrago <egon.data.datasets.storages_etrago.StorageEtrago>`
      * :py:class:`HtsEtragoTable
        <egon.data.datasets.heat_etrago.hts_etrago.HtsEtragoTable>`
      * :py:class:`ChpEtrago <egon.data.datasets.chp_etrago.ChpEtrago>`
      * :py:class:`DsmPotential <egon.data.datasets.DSM_cts_ind.DsmPotential>`
      * :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`
      * :py:class:`Egon_etrago_gen <egon.data.datasets.fill_etrago_gen.Egon_etrago_gen>`
      * :py:class:`OpenCycleGasTurbineEtrago
        <egon.data.datasets.power_etrago.OpenCycleGasTurbineEtrago>`
      * :py:class:`HydrogenStoreEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenStoreEtrago>`
      * :py:class:`HydrogenPowerLinkEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenPowerLinkEtrago>`
      * :py:class:`HydrogenMethaneLinkEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenMethaneLinkEtrago>`
      * :py:class:`GasAreaseGon100RE <egon.data.datasets.gas_areas.GasAreaseGon100RE>`
      * :py:class:`CH4Production <egon.data.datasets.ch4_prod.CH4Production>`
      * :py:class:`CH4Storages <egon.data.datasets.ch4_storages.CH4Storages>`

    *Resulting Tables*
      * :py:class:`EgonEvMitLgvPool <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvPool>`
        is created and filled
      * :py:class:`EgonEvMitLgvTrip <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvTrip>`
        is created and filled (new methodology: charging events only)
      * :py:class:`EgonEvMitLgvCountMunicipality <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountMunicipality>`
        is created and filled
      * :py:class:`EgonEvMitLgvChargingLocation <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvChargingLocation>`
        is created and filled (new methodology only)
      * :py:class:`EgonEvMitLgvLocationEvents <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvLocationEvents>`
        is created and filled (new methodology only)
      * :py:class:`EgonEvMitLgvCountRegistrationDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountRegistrationDistrict>`,
        :py:class:`EgonEvMitLgvCountMvGridDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvCountMvGridDistrict>`
        and :py:class:`EgonEvMitLgvMvGridDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMvGridDistrict>`
        are created and filled (legacy methodology only)
      * :py:class:`EgonEvMitLgvMetadata <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMitLgvMetadata>`
        is created and filled
      * :py:class:`EgonEvMitLgvFlexTimeseries <egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvFlexTimeseries>`,
        :py:class:`EgonEvMitLgvEnergyBalance <egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvEnergyBalance>`
        and :py:class:`EgonEvMitLgvChargingProfileUseCase <egon.data.datasets.emobility.motorized_individual_travel.flex_diagnostics.EgonEvMitLgvChargingProfileUseCase>`
        are created and filled (new methodology only)
      * eTraGo: `Li_ion` buses, `BEV_charger` / `BEV_charger_transmission`
        links, `battery_storage` stores and `land_transport_EV` /
        `land_transport_EV_transmission` loads with their timeseries

    *Configuration*

    Sources and targets are declared as class attributes below. The
    Zenodo records of the delivered M1+N1 input data and the switch for
    their download live in
    :mod:`egon.data.datasets.emobility.mit_lgv_input_data`.

    """

    sources = DatasetSources(
        urls={
            "KBA": "https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_2021.xlsx?__blob=publicationFile&v=2",
            "RS7": "https://www.bmv.de/SharedDocs/DE/Anlage/G/regiostar-referenzdateien.xlsx?__blob=publicationFile",
            # Delivered M1+N1 input data, one zip archive per scenario.
            **{
                f"input_data_{scenario_name}": url
                for scenario_name, url in ZENODO_URLS.items()
            },
        },
        files={
            "trips_eGon2035": "mit_trip_data/eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",
            # Files expected inside an extracted scenario archive.
            **{f"input_data_{key}": name for key, name in INPUT_FILES.items()},
            "original_data": {
                "original_data": {
                    "sources": {
                        "RS7": {
                            "url": "https://www.bmv.de/SharedDocs/DE/Anlage/G/regiostar-referenzdateien.xlsx?__blob=publicationFile",
                            "file": "regiostar-referenzdateien.xlsx",
                            "file_processed": "regiostar-referenzdateien_preprocessed.csv",
                            "sheet": "ReferenzGebietsstand2020",
                        },
                        "KBA": {
                            "url": "https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_2021.xlsx?__blob=publicationFile&v=2",
                            "file": "fz1_2021.xlsx",
                            "file_processed": "fz1_2021_preprocessed.csv",
                            "sheet": "FZ1.1",
                            "columns": "D, J:N",
                            "skiprows": 8,
                        },
                        # Trip tarballs of the legacy methodology. Only
                        # `eGon2035` remains: the new scenarios are
                        # built from the delivered Zenodo archives and
                        # must not silently fall back to the technical
                        # data of an archived simBEV run -- `bev_mini`
                        # alone differs by 70 vs. 47.5 kWh of battery
                        # capacity, which feeds `store_ev_battery.e_nom`
                        # directly.
                        "trips": {
                            "eGon2035": {
                                "file": "eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",
                                "file_metadata": "metadata_simbev_run.json",
                            },
                        },
                    },
                },
                "scenario": {
                    # Must match the scenario variation keys returned by
                    # `egon.data.datasets.scenario_parameters.parameters
                    # .mobility`
                    "variation": {
                        "status2024": "status2024",
                        "eGon2035": "NEP C 2035",
                        "reGon2037": "NEP C 2037",
                        "reGon2045": "NEP C 2045",
                    },
                    # Only scenarios modelling flexible charging get a
                    # lowflex counterpart, cf.
                    # `model_timeseries.is_flexible`
                    "lowflex": {
                        "create_lowflex_scenario": True,
                        "names": {
                            "eGon2035": "eGon2035_lowflex",
                            "reGon2037": "reGon2037_lowflex",
                            "reGon2045": "reGon2045_lowflex",
                        },
                    },
                },
                "model_timeseries": {
                    "reduce_memory": True,
                    "export_results_to_csv": True,
                    "parallel_tasks": 10,
                },
            },
        },
    )

    targets = DatasetTargets(
        files={
            "KBA_download": "emobility/fz1_2021.xlsx",
            "KBA_processed": "emobility/fz1_2021_preprocessed.csv",
            "RS7_download": "emobility/regiostar-referenzdateien.xlsx",
            "RS7_processed": (
                "emobility/regiostar-referenzdateien_preprocessed.csv"
            ),
        },
        tables={
            "ev_pool": "demand.egon_ev_mit_lgv_pool",
            "ev_trip": "demand.egon_ev_mit_lgv_trip",
            "ev_count_reg_district": (
                "demand.egon_ev_mit_lgv_count_registration_district"
            ),
            "ev_count_municipality": (
                "demand.egon_ev_mit_lgv_count_municipality"
            ),
            "ev_count_mv_grid": (
                "demand.egon_ev_mit_lgv_count_mv_grid_district"
            ),
            "ev_mv_grid": "demand.egon_ev_mit_lgv_mv_grid_district",
            "ev_charging_location": (
                "demand.egon_ev_mit_lgv_charging_location"
            ),
            "ev_location_events": "demand.egon_ev_mit_lgv_location_events",
            "ev_metadata": "demand.egon_ev_mit_lgv_metadata",
            "ev_flex_timeseries": "demand.egon_ev_mit_lgv_flex_timeseries",
            "ev_energy_balance": "demand.egon_ev_mit_lgv_energy_balance",
            "ev_charging_profile_use_case": (
                "demand.egon_ev_mit_lgv_charging_profile_use_case"
            ),
        },
    )

    #:
    name: str = "MotorizedIndividualTravel"
    #:
    version: str = "0.2.2"

    def __init__(self, dependencies):
        parallel_tasks = MotorizedIndividualTravel.sources.files[
            "original_data"
        ]["model_timeseries"].get("parallel_tasks", 1)

        def generate_model_data_tasks(scenario_name):
            """Dynamically generate the legacy model data tasks."""
            # Replicate the logic for MVGD_MIN_COUNT locally
            testmode_off = (
                config.settings()["egon-data"]["--dataset-boundary"]
                == "Everything"
            )
            mvgd_min_count = 3600 if testmode_off else 150

            mvgd_bunch_size = divmod(mvgd_min_count, parallel_tasks)[0]

            tasks = set()
            for _ in range(parallel_tasks):
                bunch = range(_ * mvgd_bunch_size, (_ + 1) * mvgd_bunch_size)
                tasks.add(
                    PythonOperator(
                        task_id=(
                            f"generate_model_data_"
                            f"{scenario_name}_"
                            f"bunch{bunch[0]}-{bunch[-1]}"
                        ),
                        python_callable=generate_model_data_bunch,
                        op_kwargs={
                            "scenario_name": scenario_name,
                            "bunch": bunch,
                        },
                    )
                )

            # Grid districts beyond the parallel bunches above are
            # processed by one additional task per scenario.
            tasks.add(
                PythonOperator(
                    task_id=(f"generate_model_data_{scenario_name}_remaining"),
                    python_callable=generate_model_data_remaining,
                    op_kwargs={"scenario_name": scenario_name},
                )
            )
            return tasks

        def generate_placement_model_tasks(scenario_name):
            """One task per share of the scenario's AC buses, cf.
            :func:`.placement_model.generate_placement_models`. The shares
            are taken by bus id, so no bus can fall between two tasks."""
            return {
                PythonOperator(
                    task_id=(
                        f"generate_model_data_{scenario_name}_"
                        f"part{part + 1}of{parallel_tasks}"
                    ),
                    python_callable=generate_placement_models,
                    op_kwargs={
                        "scenario_name": scenario_name,
                        "part": part,
                        "parts": parallel_tasks,
                    },
                )
                for part in range(parallel_tasks)
            }

        # Both methodologies are wired unconditionally: each task splits
        # the configured scenario list into legacy and new and no-ops for
        # the half it does not serve. That keeps the DAG shape
        # independent of `--scenarios`.
        tasks = (
            create_tables,
            {
                (
                    download_and_preprocess,
                    allocate_evs_numbers,
                ),
                (
                    extract_trip_file,
                    write_evs_trips_to_db,
                ),
                # New methodology. Sequential: in test mode each step
                # selects by the result of the previous one (locations
                # inside the boundary -> their placements -> the events
                # placed -> their profiles), and on a full run the imports
                # would only compete for the same database.
                (
                    download_and_extract,
                    import_charging_locations,
                    import_location_events,
                    import_charging_events,
                    import_ev_pool,
                    import_ev_counts,
                ),
            },
            write_metadata_to_db,
            allocate_evs_to_grid_districts,
            log_import_consistency,
            delete_model_data_from_db,
        )

        tasks_per_scenario = set()

        for scenario_name in config.settings()["egon-data"]["--scenarios"]:
            tasks_per_scenario.update(
                generate_model_data_tasks(scenario_name=scenario_name)
                if is_legacy_scenario(scenario_name)
                else generate_placement_model_tasks(scenario_name)
            )

        tasks = tasks + (
            tasks_per_scenario,
            log_diagnostics_checks,
            add_metadata,
        )

        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
            validation={"data-quality": mit_validation_rules()},
            proceed_on_validation_failure=True,
        )
