"""
Motorized Individual Travel (MIT)

Main module for preparation of model data (static and timeseries) for
motorized individual travel.

**Contents of this module**
* Creation of DB tables
* Download and preprocessing of vehicle registration data from KBA and BMVI
* Calculate number of electric vehicles and allocate on different spatial
  levels. See :py:mod:`egon.data.metadata`
* Extract and write pre-generated trips to DB

**Configuration**

The config of this dataset can be found in *datasets.yml* in section
*emobility_mit*.

**Scenarios and variations**

* Scenario overview
* Change scenario variation for 2050: adjust in
emobility_mit->scenario->variation->eGon100RE

**Trip data**

The electric vehicles' trip data for each scenario have been generated using
`simBEV <https://github.com/rl-institut/simbev/>`_. The methodical background
is given in its `documentation <https://simbev.readthedocs.io>`_.

6 different vehicle types are used:
* Battery Electric Vehicle (BEV): mini, medium, luxury
* Plug-in Hybrid Electric Vehicle (PHEV): mini, medium, luxury

.. csv-table:: EV types
    :header: "Tecnnology", "Size", "Max. charging capacity slow [kW]",
             "Max. charging capacity fast [kW]", "Battery capacity [kWh]",
             "Energy consumption [kWh/km]"
    :widths: 10, 10, 30, 30, 25, 30

    "BEV", "mini", 11, 120, 60, 0.1397
    "BEV", "medium", 22, 350, 90, 0.1746
    "BEV", "luxury", 50, 350, 110, 0.2096
    "PHEV", "mini", 3.7, 40, 14, 0.1425
    "PHEV", "medium", 11, 40, 20, 0.1782
    "PHEV", "luxury", 11, 120, 30, 0.2138

The complete tech data and assumptions of the run can be found in the metadata:
*<WORKING_DIRECTORY>/data_bundle_egon_data/emobility/mit_trip_data/<SCENARIO>/
metadata_simbev_run.json*.efficiency_fixed

* explain scenario parameters

* run params (all in meta file?)

**EV allocation**

The EVs per registration district (Zulassungsbezirk) is taken from KBA's
vehicle registration data. The numbers per EV type (BEV and PHEV)

* RegioStaR7
* scenario parameters: shares

**Further notes**

* Sanity checks

**Model paametrization**

**Example queries**


"""

from pathlib import Path
from urllib.request import urlretrieve
import os
import tarfile

from airflow.operators.python_operator import PythonOperator
from psycopg2.extensions import AsIs, register_adapter
import numpy as np
import pandas as pd

from egon.data import config, db, subprocess
from egon.data.datasets import Dataset
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
    DATASET_CFG,
    MVGD_MIN_COUNT,
    TESTMODE_OFF,
    TRIP_COLUMN_MAPPING,
    WORKING_DIR,
)
from egon.data.datasets.emobility.motorized_individual_travel.model_timeseries import (  # noqa: E501
    delete_model_data_from_db,
    generate_model_data_bunch,
    generate_model_data_status2019_remaining,
    generate_model_data_eGon100RE_remaining,
    generate_model_data_eGon2035_remaining,
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

    mit_sources = DATASET_CFG["original_data"]["sources"]

    # Create the folder, if it does not exist
    if not os.path.exists(WORKING_DIR):
        os.mkdir(WORKING_DIR)

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

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
        print(f"SCENARIO: {scenario_name}")
        trip_file = trip_dir / Path(
            DATASET_CFG["original_data"]["sources"]["trips"][scenario_name][
                "file"
            ]
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

    def import_csv(f):
        df = pd.read_csv(f, usecols=TRIP_COLUMN_MAPPING.keys())
        df["rs7_id"] = int(f.parent.name)
        df["simbev_ev_id"] = "_".join(f.name.split("_")[0:3])
        return df

    for scenario_name in config.settings()["egon-data"]["--scenarios"]:
        print(f"SCENARIO: {scenario_name}")
        trip_dir_name = Path(
            DATASET_CFG["original_data"]["sources"]["trips"][scenario_name][
                "file"
            ].split(".")[0]
        )

        trip_dir_root = DATA_BUNDLE_DIR / Path("mit_trip_data", trip_dir_name)

        if TESTMODE_OFF:
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
    def __init__(self, dependencies):
        def generate_model_data_tasks(scenario_name):
            """Dynamically generate tasks for model data creation.

            The goal is to speed up the creation of model timeseries. However,
            the exact number of parallel task cannot be determined during the
            DAG building as the number of grid districts (MVGD) is calculated
            within another pipeline task.
            Approach: assuming an approx. count of `mvgd_min_count` of 3700,
            the majority of the MVGDs can be parallelized. The remainder is
            handled subsequently in XXX.
            The number of parallel tasks is defined via parameter
            `parallel_tasks` in the dataset config `datasets.yml`.

            Parameters
            ----------
            scenario_name : str
                Scenario name

            Returns
            -------
            set of functools.partial
                The tasks. Each element is of
                :func:`egon.data.datasets.emobility.motorized_individual_travel.model_timeseries.generate_model_data`
            """
            parallel_tasks = DATASET_CFG["model_timeseries"].get(
                "parallel_tasks", 1
            )
            mvgd_bunch_size = divmod(MVGD_MIN_COUNT, parallel_tasks)[0]

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
                delete_model_data_from_db, )
    
        tasks_per_scenario = set()
        
        for scenario_name in config.settings()["egon-data"]["--scenarios"]:
            tasks_per_scenario.update(generate_model_data_tasks(scenario_name=scenario_name))
            
        tasks = tasks +  (tasks_per_scenario,)
        
        super().__init__(
            name="MotorizedIndividualTravel",
            version="0.0.7",
            dependencies=dependencies,
            tasks=tasks
        )
