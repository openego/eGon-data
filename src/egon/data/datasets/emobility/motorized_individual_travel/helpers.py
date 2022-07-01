"""
Helpers: constants and functions for motorized individual travel
"""

from pathlib import Path
import json

import numpy as np
import pandas as pd

import egon.data.config

TESTMODE_OFF = (
    egon.data.config.settings()["egon-data"]["--dataset-boundary"]
    == "Everything"
)
WORKING_DIR = Path(".", "emobility")
DATA_BUNDLE_DIR = Path(
    ".",
    "data_bundle_egon_data",
    "emobility",
)
DATASET_CFG = egon.data.config.datasets()["emobility_mit"]
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
MVGD_MIN_COUNT = 3700 if TESTMODE_OFF else 150


def read_kba_data():
    """Read KBA data from CSV"""
    return pd.read_csv(
        WORKING_DIR
        / egon.data.config.datasets()["emobility_mit"]["original_data"][
            "sources"
        ]["KBA"]["file_processed"]
    )


def read_rs7_data():
    """Read RegioStaR7 data from CSV"""
    return pd.read_csv(
        WORKING_DIR
        / egon.data.config.datasets()["emobility_mit"]["original_data"][
            "sources"
        ]["RS7"]["file_processed"]
    )


def read_simbev_metadata_file(scenario_name, section):
    """Read metadata of simBEV run

    Parameters
    ----------
    scenario_name : str
        Scenario name
    section : str
        Metadata section to be returned, one of
        * "tech_data"
        * "charge_prob_slow"
        * "charge_prob_fast"

    Returns
    -------
    pd.DataFrame
        Config data
    """
    trips_cfg = DATASET_CFG["original_data"]["sources"]["trips"]
    meta_file = DATA_BUNDLE_DIR / Path(
        "mit_trip_data",
        trips_cfg[scenario_name]["file"].split(".")[0],
        trips_cfg[scenario_name]["file_metadata"],
    )
    with open(meta_file) as f:
        meta = json.loads(f.read())
    return pd.DataFrame.from_dict(meta.get(section, dict()), orient="index")


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
    start_mem = df.memory_usage().sum() / 1024 ** 2

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

    end_mem = df.memory_usage().sum() / 1024 ** 2

    if show_reduction is True:
        print(
            "Reduced memory usage of DataFrame by "
            f"{(1 - end_mem/start_mem) * 100:.2f} %."
        )

    return df
