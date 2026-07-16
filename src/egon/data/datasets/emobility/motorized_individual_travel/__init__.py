"""
Main module for preparation of model data (static and timeseries) for
motorized individual travel (MIT).

**Contents of this module**
  * Creation of DB tables
  * Download and preprocessing of vehicle registration data from KBA and BMVI
  * Calculate number of electric vehicles and allocate on different spatial
    levels.
  * Extract and write pre-generated trips to DB

"""

from pathlib import Path
from urllib.request import urlretrieve
import os
import tarfile

from airflow.operators.python import PythonOperator
from psycopg2.extensions import AsIs, register_adapter
import numpy as np
import pandas as pd

from egon.data import config, db, subprocess
from egon.data.datasets import Dataset, DatasetSources, DatasetTargets
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvCountMunicipality,
    EgonEvCountMvGridDistrict,
    EgonEvCountRegistrationDistrict,
    EgonEvMetadata,
    EgonEvMvGridDistrict,
    EgonEvPool,
    EgonEvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.ev_allocation import (  # noqa: E501
    allocate_evs_numbers,
    allocate_evs_to_grid_districts,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    COLUMNS_KBA,
    DATA_BUNDLE_DIR,
    TRIP_COLUMN_MAPPING,
    WORKING_DIR,
)
from egon.data.datasets.emobility.motorized_individual_travel.model_timeseries import (  # noqa: E501
    delete_model_data_from_db,
    generate_model_data_bunch,
    generate_model_data_eGon100RE_remaining,
    generate_model_data_eGon2035_remaining,
    generate_model_data_status2019_remaining,
    generate_model_data_status2023_remaining,
    read_simbev_metadata_file,
)


# ========== Register np datatypes with SQLA ==========
def adapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)


def adapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)


register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.int64, adapt_numpy_int64)
# =====================================================


def create_tables():
    """Create tables for electric vehicles

    Returns
    -------
    None
    """

    engine = db.engine()
    EgonEvCountRegistrationDistrict.__table__.drop(
        bind=engine, checkfirst=True
    )
    EgonEvCountRegistrationDistrict.__table__.create(
        bind=engine, checkfirst=True
    )
    EgonEvCountMunicipality.__table__.drop(bind=engine, checkfirst=True)
    EgonEvCountMunicipality.__table__.create(bind=engine, checkfirst=True)
    EgonEvCountMvGridDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvCountMvGridDistrict.__table__.create(bind=engine, checkfirst=True)
    EgonEvPool.__table__.drop(bind=engine, checkfirst=True)
    EgonEvPool.__table__.create(bind=engine, checkfirst=True)
    EgonEvTrip.__table__.drop(bind=engine, checkfirst=True)
    EgonEvTrip.__table__.create(bind=engine, checkfirst=True)
    EgonEvMvGridDistrict.__table__.drop(bind=engine, checkfirst=True)
    EgonEvMvGridDistrict.__table__.create(bind=engine, checkfirst=True)
    EgonEvMetadata.__table__.drop(bind=engine, checkfirst=True)
    EgonEvMetadata.__table__.create(bind=engine, checkfirst=True)

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
    """Extract trip file from data bundle"""
    trip_dir = DATA_BUNDLE_DIR / Path("mit_trip_data")

    mit_sources = MotorizedIndividualTravel.sources.files["original_data"][
        "original_data"
    ]["sources"]

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
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

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
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
        trip_data["egon_ev_pool_ev_id"] = pd.merge(
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
            name=EgonEvPool.__table__.name,
            schema=EgonEvPool.__table__.schema,
            con=db.engine(),
            if_exists="append",
            index=True,
        )

        # Write trips to CSV and import to DB
        print("Writing EV trips to CSV file...")
        trip_file = WORKING_DIR / f"trip_data_{scenario_name}.csv"
        trip_data.to_csv(trip_file)

        # Get DB config
        docker_db_config = db.credentials()
        host = ["-h", f"{docker_db_config['HOST']}"]
        port = ["-p", f"{docker_db_config['PORT']}"]
        pgdb = ["-d", f"{docker_db_config['POSTGRES_DB']}"]
        user = ["-U", f"{docker_db_config['POSTGRES_USER']}"]
        command = [
            "-c",
            rf"\copy {EgonEvTrip.__table__.schema}.{EgonEvTrip.__table__.name}"
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
    """
    Write used SimBEV metadata per scenario to database.
    """
    dtypes = {
        "scenario": str,
        "eta_cp": float,
        "stepsize": int,
        "start_date": "datetime64[ns]",
        "end_date": "datetime64[ns]",
        "soc_min": float,
        "grid_timeseries": bool,
        "grid_timeseries_by_usecase": bool,
    }

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
        meta_run_config = read_simbev_metadata_file(
            scenario_name, "config"
        ).loc["basic"]

        meta_run_config = (
            meta_run_config.to_frame()
            .T.assign(scenario=scenario_name)[dtypes.keys()]
            .astype(dtypes)
        )

        meta_run_config.to_sql(
            name=EgonEvMetadata.__table__.name,
            schema=EgonEvMetadata.__table__.schema,
            con=db.engine(),
            if_exists="append",
            index=False,
        )


class MotorizedIndividualTravel(Dataset):
    """
    Class to set up static and timeseries data for motorized individual travel (MIT).  # noqa: E501

    For more information see data documentation on :ref:`mobility-demand-mit-ref`.  # noqa: E501

    *Dependencies*
      * :py:class:`DataBundle <egon.data.datasets.data_bundle.DataBundle>`
      * :py:class:`MvGridDistricts
        <egon.data.datasets.mv_grid_districts.mv_grid_districts_setup>`
      * :py:class:`ScenarioParameters
        <egon.data.datasets.scenario_parameters.ScenarioParameters>`
      * :py:class:`EtragoSetup <egon.data.datasets.etrago_setup.EtragoSetup>`
      * :py:class:`ZensusMvGridDistricts
        <egon.data.datasets.zensus_mv_grid_districts.ZensusMvGridDistricts>`
      * :py:class:`ZensusVg250 <egon.data.datasets.zensus_vg250.ZensusVg250>`
      * :py:class:`StorageEtrago <egon.data.datasets.storages_etrago.StorageEtrago>`  # noqa: E501
      * :py:class:`HtsEtragoTable
        <egon.data.datasets.heat_etrago.hts_etrago.HtsEtragoTable>`
      * :py:class:`ChpEtrago <egon.data.datasets.chp_etrago.ChpEtrago>`
      * :py:class:`DsmPotential <egon.data.datasets.DSM_cts_ind.DsmPotential>`
      * :py:class:`HeatEtrago <egon.data.datasets.heat_etrago.HeatEtrago>`
      * :py:class:`Egon_etrago_gen <egon.data.datasets.fill_etrago_gen.Egon_etrago_gen>`  # noqa: E501
      * :py:class:`OpenCycleGasTurbineEtrago
        <egon.data.datasets.power_etrago.OpenCycleGasTurbineEtrago>`
      * :py:class:`HydrogenStoreEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenStoreEtrago>`
      * :py:class:`HydrogenPowerLinkEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenPowerLinkEtrago>`
      * :py:class:`HydrogenMethaneLinkEtrago
        <egon.data.datasets.hydrogen_etrago.HydrogenMethaneLinkEtrago>`
      * :py:class:`GasAreaseGon100RE <egon.data.datasets.gas_areas.GasAreaseGon100RE>`  # noqa: E501
      * :py:class:`CH4Production <egon.data.datasets.ch4_prod.CH4Production>`
      * :py:class:`CH4Storages <egon.data.datasets.ch4_storages.CH4Storages>`

    *Resulting Tables*
      * :py:class:`EgonEvPool <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvPool>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvTrip <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvTrip>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvCountRegistrationDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvCountRegistrationDistrict>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvCountMunicipality <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvCountMunicipality>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvCountMvGridDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvCountMvGridDistrict>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvMvGridDistrict <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMvGridDistrict>`  # noqa: E501
        is created and filled
      * :py:class:`EgonEvMetadata <egon.data.datasets.emobility.motorized_individual_travel.db_classes.EgonEvMetadata>`  # noqa: E501
        is created and filled

    *Configuration*

    The config of this dataset can be found in *datasets.yml* in section
    *emobility_mit*.

    """

    sources = DatasetSources(
        urls={
            "KBA": "https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_2021.xlsx?__blob=publicationFile&v=2",  # noqa: E501
            "RS7": "https://www.bmv.de/SharedDocs/DE/Anlage/G/regiostar-referenzdateien.xlsx?__blob=publicationFile",  # noqa: E501
        },
        files={
            "trips_status2019": "mit_trip_data/eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
            "trips_status2023": "mit_trip_data/eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
            "trips_eGon2035": "mit_trip_data/eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
            "trips_eGon100RE": "mit_trip_data/eGon100RE_RS7_min2k_2022-06-01_175444_simbev_run.tar.gz",  # noqa: E501
            "original_data": {
                "original_data": {
                    "sources": {
                        "RS7": {
                            "url": "https://www.bmv.de/SharedDocs/DE/Anlage/G/regiostar-referenzdateien.xlsx?__blob=publicationFile",  # noqa: E501
                            "file": "regiostar-referenzdateien.xlsx",
                            "file_processed": "regiostar-referenzdateien_preprocessed.csv",  # noqa: E501
                            "sheet": "ReferenzGebietsstand2020",
                        },
                        "KBA": {
                            "url": "https://www.kba.de/SharedDocs/Downloads/DE/Statistik/Fahrzeuge/FZ1/fz1_2021.xlsx?__blob=publicationFile&v=2",  # noqa: E501
                            "file": "fz1_2021.xlsx",
                            "file_processed": "fz1_2021_preprocessed.csv",
                            "sheet": "FZ1.1",
                            "columns": "D, J:N",
                            "skiprows": 8,
                        },
                        "trips": {
                            "status2019": {
                                "file": "eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
                                "file_metadata": "metadata_simbev_run.json",
                            },
                            "status2023": {
                                "file": "eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
                                "file_metadata": "metadata_simbev_run.json",
                            },
                            "eGon2035": {
                                "file": "eGon2035_RS7_min2k_2022-06-01_175429_simbev_run.tar.gz",  # noqa: E501
                                "file_metadata": "metadata_simbev_run.json",
                            },
                            "eGon100RE": {
                                "file": "eGon100RE_RS7_min2k_2022-06-01_175444_simbev_run.tar.gz",  # noqa: E501
                                "file_metadata": "metadata_simbev_run.json",
                            },
                        },
                    },
                },
                "scenario": {
                    "variation": {
                        "status2019": "status2019",
                        "status2023": "status2023",
                        "eGon2035": "NEP C 2035",
                        "eGon100RE": "Reference 2050",
                    },
                    "lowflex": {
                        "create_lowflex_scenario": True,
                        "names": {
                            "eGon2035": "eGon2035_lowflex",
                            "eGon100RE": "eGon100RE_lowflex",
                        },
                    },
                },
                "model_timeseries": {
                    "reduce_memory": True,
                    "export_results_to_csv": True,
                    "parallel_tasks": 10,
                },
                "demand_timeseries_mvgd": {
                    "parallel_tasks": 10,
                },
            },
        },
    )

    targets = DatasetTargets(
        files={
            "KBA_download": "motorized_individual_travel/fz1_2021.xlsx",
            "KBA_processed": "motorized_individual_travel/fz1_2021_preprocessed.csv",  # noqa: E501
            "RS7_download": "motorized_individual_travel/regiostar-referenzdateien.xlsx",  # noqa: E501
            "RS7_processed": "motorized_individual_travel/regiostar-referenzdateien_preprocessed.csv",  # noqa: E501
        },
        tables={
            "ev_pool": "emobility.egon_ev_pool",
            "ev_trip": "emobility.egon_ev_trip",
            "ev_count_reg_district": "emobility.egon_ev_count_registration_district",  # noqa: E501
            "ev_count_municipality": "emobility.egon_ev_count_municipality",
            "ev_count_mv_grid": "emobility.egon_ev_count_mv_grid_district",
            "ev_mv_grid": "emobility.egon_ev_mv_grid_district",
            "ev_metadata": "emobility.egon_ev_metadata",
        },
    )

    #:
    name: str = "MotorizedIndividualTravel"
    #:
    version: str = "0.0.11"

    def __init__(self, dependencies):
        def generate_model_data_tasks(scenario_name):
            """Dynamically generate tasks for model data creation."""
            # Use class attributes directly
            mit_original_data = MotorizedIndividualTravel.sources.files[
                "original_data"
            ]

            parallel_tasks = mit_original_data["model_timeseries"].get(
                "parallel_tasks", 1
            )

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

            if scenario_name == "status2019":
                tasks.add(generate_model_data_status2019_remaining)
            if scenario_name == "status2023":
                tasks.add(generate_model_data_status2023_remaining)
            elif scenario_name == "eGon2035":
                tasks.add(generate_model_data_eGon2035_remaining)
            elif scenario_name == "eGon100RE":
                tasks.add(generate_model_data_eGon100RE_remaining)
            return tasks

        tasks = (
            create_tables,
            {
                (
                    download_and_preprocess,
                    allocate_evs_numbers,
                ),
                (
                    extract_trip_file,
                    write_metadata_to_db,
                    write_evs_trips_to_db,
                ),
            },
            allocate_evs_to_grid_districts,
            delete_model_data_from_db,
        )

        tasks_per_scenario = set()

        for scenario_name in config.settings()["egon-data"]["--scenarios"]:
            tasks_per_scenario.update(
                generate_model_data_tasks(scenario_name=scenario_name)
            )

        tasks = tasks + (tasks_per_scenario,)

        super().__init__(
            name=self.name,
            version=self.version,
            dependencies=dependencies,
            tasks=tasks,
        )
