import json
import pandas as pd
import numpy as np
from pathlib import Path
import egon.data.config

TESTMODE_OFF = (
    egon.data.config.settings()["egon-data"]["--dataset-boundary"] ==
    "Everything"
)
WORKING_DIR = Path(".", "emobility")
DATA_BUNDLE_DIR = Path(
    ".", "data_bundle_egon_data", "emobility",
)
COLUMNS_KBA = [
    'reg_district',
    'total',
    'mini',
    'medium',
    'luxury',
    'unknown',
]
CONFIG_EV = {
    'bev_mini': {'column': 'mini',
                 'tech_share': 'bev_mini_share',
                 'share': 'mini_share',
                 'factor': 'mini_factor'},
    'bev_medium': {'column': 'medium',
                   'tech_share': 'bev_medium_share',
                   'share': 'medium_share',
                   'factor': 'medium_factor'},
    'bev_luxury': {'column': 'luxury',
                   'tech_share': 'bev_luxury_share',
                   'share': 'luxury_share',
                   'factor': 'luxury_factor'},
    'phev_mini': {'column': 'mini',
                  'tech_share': 'phev_mini_share',
                  'share': 'mini_share',
                  'factor': 'mini_factor'},
    'phev_medium': {'column': 'medium',
                    'tech_share': 'phev_medium_share',
                    'share': 'medium_share',
                    'factor': 'medium_factor'},
    'phev_luxury': {'column': 'luxury',
                    'tech_share': 'phev_luxury_share',
                    'share': 'luxury_share',
                    'factor': 'luxury_factor'},
}
TRIP_COLUMN_MAPPING = {
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
    "consumption_kWh": "consumption"
}


def read_kba_data():
    """Read KBA data from CSV"""
    return pd.read_csv(
        WORKING_DIR /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["KBA"]["file_processed"]
    )


def read_rs7_data():
    """Read RegioStaR7 data from CSV"""
    return pd.read_csv(
        WORKING_DIR /
        egon.data.config.datasets()[
            "emobility_mit"]["original_data"][
            "sources"]["RS7"]["file_processed"]
    )


def read_simbev_metadata_file(section):
    """Read metadata of simBEV run

    Parameters
    ----------
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
    trips_cfg = egon.data.config.datasets()["emobility_mit"][
            "original_data"]["sources"]["trips"]
    meta_file = DATA_BUNDLE_DIR / Path(
        "mit_trip_data",
        trips_cfg["file"].split(".")[0],
        trips_cfg["file_metadata"]
    )
    with open(meta_file) as f:
        meta = json.loads(f.read())
    return pd.DataFrame.from_dict(meta.get(section, dict()), orient='index')
