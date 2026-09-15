"""
Helpers: constants and functions for motorized individual travel
"""

from pathlib import Path
import json

import numpy as np
import pandas as pd

from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.emobility.mit_lgv_input_data import (  # noqa: F401
    GEOLIS_METADATA_FILE,
    INPUT_DATA_DIR,
    LEGACY_SCENARIOS,
    SIMBEV_METADATA_FILE,
    WORKING_DIR,
    is_legacy_scenario,
    legacy_scenarios,
    new_methodology_scenarios,
    scenario_input_dir,
)
import egon.data.config

TESTMODE_OFF = (
    egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    == "Everything"
)
DATA_BUNDLE_DIR = Path(
    ".",
    "data_bundle_egon_data",
    "emobility",
)
COLUMNS_KBA = [
    "reg_district",
    "total",
    "mini",
    "medium",
    "luxury",
    "unknown",
]
CONFIG_EV = {
    "bev_mini": {
        "column": "mini",
        "tech_share": "bev_mini_share",
        "share": "mini_share",
        "factor": "mini_factor",
    },
    "bev_medium": {
        "column": "medium",
        "tech_share": "bev_medium_share",
        "share": "medium_share",
        "factor": "medium_factor",
    },
    "bev_luxury": {
        "column": "luxury",
        "tech_share": "bev_luxury_share",
        "share": "luxury_share",
        "factor": "luxury_factor",
    },
    "phev_mini": {
        "column": "mini",
        "tech_share": "phev_mini_share",
        "share": "mini_share",
        "factor": "mini_factor",
    },
    "phev_medium": {
        "column": "medium",
        "tech_share": "phev_medium_share",
        "share": "medium_share",
        "factor": "medium_factor",
    },
    "phev_luxury": {
        "column": "luxury",
        "tech_share": "phev_luxury_share",
        "share": "luxury_share",
        "factor": "luxury_factor",
    },
}
#: The six privately registered M1 types of both methodologies.
EV_TYPES_PRIVATE = tuple(CONFIG_EV.keys())
#: Commercially registered *passenger cars* (M1), new methodology only.
EV_TYPES_COMMERCIAL = ("bev_commercial", "phev_commercial")
#: Light commercial vehicles (N1, < 3.5 t), new methodology only. N1 is
#: BEV-only, there is no `phev_light_duty_vehicle`.
EV_TYPES_LGV = ("bev_light_duty_vehicle",)
#: All nine vehicle types of the new methodology (D13).
EV_TYPES = EV_TYPES_PRIVATE + EV_TYPES_COMMERCIAL + EV_TYPES_LGV

#: Charging use cases eligible for flexible (smart) charging in the new
#: methodology (D10). This replaces the `(location, use_case)` pair test
#: of the legacy path.
FLEX_USE_CASES = ("depot", "home_detached", "home_apartment", "work")
#: Charging use cases that are always charged dumb.
INFLEX_USE_CASES = ("street", "retail", "urban_fast", "highway_fast")
#: The eight delivered `charging_use_case` values. One vocabulary for
#: all vehicle groups.
CHARGING_USE_CASES = FLEX_USE_CASES + INFLEX_USE_CASES

#: `ev_event` column names as delivered, mapped to the columns of
#: `demand.egon_ev_mit_lgv_trip`. `charging_use_case` becomes `use_case`
#: (D12); `park_time_timesteps` is not imported -- it is
#: `park_end - park_start` and the model code ignores it (D16).
EVENT_COLUMN_MAPPING = {
    "event_id": "event_id",
    "ev_id": "ev_id",
    "charging_use_case": "use_case",
    "location": "location",
    "nominal_charging_capacity_kW": "charging_capacity_nominal",
    "grid_charging_capacity_kW": "charging_capacity_grid",
    "battery_charging_capacity_kW": "charging_capacity_battery",
    "soc_start": "soc_start",
    "soc_end": "soc_end",
    "chargingdemand_kWh": "charging_demand",
    "park_start_timesteps": "park_start",
    "park_end_timesteps": "park_end",
    "drive_start_timesteps": "drive_start",
    "drive_end_timesteps": "drive_end",
    "consumption_kWh": "consumption",
}

TRIP_COLUMN_MAPPING = {
    "location": "location",
    "use_case": "use_case",
    "nominal_charging_capacity_kW": "charging_capacity_nominal",
    "grid_charging_capacity_kW": "charging_capacity_grid",
    "battery_charging_capacity_kW": "charging_capacity_battery",
    "soc_start": "soc_start",
    "soc_end": "soc_end",
    "chargingdemand_kWh": "charging_demand",
    "park_start_timesteps": "park_start",
    "park_end_timesteps": "park_end",
    "drive_start_timesteps": "drive_start",
    "drive_end_timesteps": "drive_end",
    "consumption_kWh": "consumption",
}
MVGD_MIN_COUNT = 3600 if TESTMODE_OFF else 150


def read_kba_data():
    """Read KBA data from CSV"""
    sources, targets = load_sources_and_targets("MotorizedIndividualTravel")
    file_processed = sources.files["original_data"]["original_data"][
        "sources"
    ]["KBA"]["file_processed"]

    return pd.read_csv(WORKING_DIR / file_processed)


def read_rs7_data():
    """Read RegioStaR7 data from CSV"""
    sources, targets = load_sources_and_targets("MotorizedIndividualTravel")
    file_processed = sources.files["original_data"]["original_data"][
        "sources"
    ]["RS7"]["file_processed"]

    return pd.read_csv(WORKING_DIR / file_processed)


def simbev_metadata_path(scenario_name):
    """Path of the simBEV run metadata of a scenario.

    This is the branch point between the two methodologies: legacy
    scenarios read the metadata shipped inside the trip tarball of the
    data bundle, new ones the one delivered with the Zenodo archive.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    pathlib.Path
        Path of `metadata_simbev_run.json`
    """
    if not is_legacy_scenario(scenario_name):
        return scenario_input_dir(scenario_name) / SIMBEV_METADATA_FILE

    sources, targets = load_sources_and_targets("MotorizedIndividualTravel")
    trips_cfg = sources.files["original_data"]["original_data"]["sources"][
        "trips"
    ]

    return DATA_BUNDLE_DIR / Path(
        "mit_trip_data",
        trips_cfg[scenario_name]["file"].split(".")[0],
        trips_cfg[scenario_name]["file_metadata"],
    )


def read_simbev_metadata_file(scenario_name, section):
    """Read metadata of simBEV run

    Parameters
    ----------
    scenario_name : str
        Scenario name
    section : str
        Metadata section to be returned, one of
        * "config"
        * "tech_data"
        * "charge_prob_slow"
        * "charge_prob_fast"

    Returns
    -------
    pd.DataFrame
        Config data
    """
    meta_file = simbev_metadata_path(scenario_name)
    if not meta_file.is_file():
        raise FileNotFoundError(
            f"simBEV metadata for scenario '{scenario_name}' not found "
            f"at {meta_file}. Run the input data download first."
        )
    with open(meta_file) as f:
        meta = json.loads(f.read())
    return pd.DataFrame.from_dict(meta.get(section, dict()), orient="index")


def read_geolis_metadata_file(scenario_name):
    """Read the run configuration of the GeoLIS run of a scenario.

    Only delivered for the new methodology; legacy scenarios ship no
    GeoLIS file.

    Parameters
    ----------
    scenario_name : str
        Scenario name

    Returns
    -------
    dict
        Contents of `metadata_geolis_run.json`
    """
    meta_file = scenario_input_dir(scenario_name) / GEOLIS_METADATA_FILE
    if not meta_file.is_file():
        raise FileNotFoundError(
            f"GeoLIS metadata for scenario '{scenario_name}' not found "
            f"at {meta_file}. Run the input data download first."
        )
    with open(meta_file) as f:
        return json.loads(f.read())


def reduce_mem_usage(
    df: pd.DataFrame, show_reduction: bool = False
) -> pd.DataFrame:
    """Function to automatically check if columns of a pandas DataFrame can
    be reduced to a smaller data type. Source:
    https://www.mikulskibartosz.name/how-to-reduce-memory-usage-in-pandas/

    Parameters
    ----------
    df: pd.DataFrame
        DataFrame to reduce memory usage on
    show_reduction : bool
        If True, print amount of memory reduced

    Returns
    -------
    pd.DataFrame
        DataFrame with memory usage decreased
    """
    start_mem = df.memory_usage().sum() / 1024**2

    for col in df.columns:
        col_type = df[col].dtype

        if col_type != object and str(col_type) != "category":
            c_min = df[col].min()
            c_max = df[col].max()

            if str(col_type)[:3] == "int":
                if (
                    c_min > np.iinfo(np.int16).min
                    and c_max < np.iinfo(np.int16).max
                ):
                    df[col] = df[col].astype("int16")
                elif (
                    c_min > np.iinfo(np.int32).min
                    and c_max < np.iinfo(np.int32).max
                ):
                    df[col] = df[col].astype("int32")
                else:
                    df[col] = df[col].astype("int64")
            else:
                if (
                    c_min > np.finfo(np.float32).min
                    and c_max < np.finfo(np.float32).max
                ):
                    df[col] = df[col].astype("float32")
                else:
                    df[col] = df[col].astype("float64")

        else:
            df[col] = df[col].astype("category")

    end_mem = df.memory_usage().sum() / 1024**2

    if show_reduction is True:
        print(
            "Reduced memory usage of DataFrame by "
            f"{(1 - end_mem/start_mem) * 100:.2f} %."
        )

    return df
