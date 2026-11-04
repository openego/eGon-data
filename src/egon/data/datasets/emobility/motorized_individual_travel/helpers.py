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

#: Model a charging location belongs to in the eTraGo model, cf.
#: :func:`voltage_level`: the aggregate of all distribution grid
#: locations of one MV grid district, or a model of its own for a grid
#: connection in the transmission grid.
MODEL_DISTRICT = "district"
MODEL_TRANSMISSION = "transmission"
MODEL_TYPES = (MODEL_DISTRICT, MODEL_TRANSMISSION)

#: eTraGo carriers per model type. The transmission model needs its own
#: link and load carriers: eTraGo's distribution grid separation and
#: eGo's hand-over to eDisGo select `BEV_charger` and
#: `land_transport_EV` by carrier only (ADR-0003). Bus and store keep
#: theirs, so all e-mobility store handling in eTraGo applies unchanged.
BUS_CARRIER = "Li_ion"
STORE_CARRIER = "battery_storage"
LINK_CARRIERS = {
    MODEL_DISTRICT: "BEV_charger",
    MODEL_TRANSMISSION: "BEV_charger_transmission",
}
LOAD_CARRIERS = {
    MODEL_DISTRICT: "land_transport_EV",
    MODEL_TRANSMISSION: "land_transport_EV_transmission",
}

#: Lower bounds (exclusive, MW) of the voltage levels 1-6 by installed
#: capacity of a grid connection; everything up to 0.1 MW is level 7.
#: The thresholds of the HGV charging and public bus datasets and of
#: `power_plants.assign_voltage_level_by_capacity`. There is no level 2
#: by capacity.
VOLTAGE_LEVEL_THRESHOLDS_MW = (
    (120.0, 1),
    (20.0, 3),
    (5.5, 4),
    (0.2, 5),
    (0.1, 6),
)
#: Highest voltage level (lowest number) still modelled in the district
#: model; levels 1-3 are transmission grid.
DISTRICT_MODEL_MIN_LEVEL = 4


def voltage_level_sql(capacity_mw: str) -> str:
    """SQL `CASE` expression mapping an installed capacity to a level.

    Parameters
    ----------
    capacity_mw : str
        SQL expression of the installed capacity in MW

    Returns
    -------
    str
        SQL expression of the voltage level (1-7)
    """
    cases = " ".join(
        f"WHEN {capacity_mw} > {threshold} THEN {level}"
        for threshold, level in VOLTAGE_LEVEL_THRESHOLDS_MW
    )
    return f"(CASE {cases} ELSE 7 END)"


#: `ev_event` column names as delivered, mapped to the columns of
#: `demand.egon_ev_mit_lgv_trip`. `charging_use_case` becomes `use_case`
#: (D12); `park_time_timesteps` is not imported -- it is
#: `park_end - park_start` and the model code ignores it (D16). The
#: `park_end` of `street` events is shortened on import
#: (:func:`limit_street_park_time`).
EVENT_COLUMN_MAPPING = {
    "event_id": "event_id",
    "ev_id": "ev_id",
    "charging_use_case": "use_case",
    "location": "location",
    "nominal_charging_capacity_kW": "charging_capacity_nominal",
    "soc_start": "soc_start",
    "soc_end": "soc_end",
    "chargingdemand_battery_kWh": "charging_demand",
    "park_start_timesteps": "park_start",
    "park_end_timesteps": "park_end",
    "drive_start_timesteps": "drive_start",
    "drive_end_timesteps": "drive_end",
    "consumption_kWh": "consumption",
}

#: Columns of `demand.egon_ev_mit_lgv_trip` filled from a delivered
#: column that is already mapped elsewhere in `EVENT_COLUMN_MAPPING`.
#: Since the delivered nameplate power *is* the grid-side power, so
#: both columns carry it.
#:
#: `charging_capacity_battery` is deliberately absent from both dicts
#: and stays NULL for the new methodology: V2 delivers no battery-side
#: nameplate power, and nothing in the model reads the column.
EVENT_COLUMN_ALIASES = {
    "charging_capacity_grid": "nominal_charging_capacity_kW",
}

#: Use case whose parking window is shortened on import, cf.
#: :func:`limit_street_park_time`.
STREET_USE_CASE = "street"
#: Timesteps per day of the delivered 15-minute resolution.
TIMESTEPS_PER_DAY = 96
#: GeoLIS' parking time limitation of `street` events, in timesteps of
#: the day: between 9:00 and 21:00 a car occupies its point for at most
#: 4 h, or for its charging time if that is longer.
STREET_PARK_LIMIT_TIMESTEPS = 16
STREET_PARK_LIMIT_WINDOW = (36, 84)


def limit_street_park_time(park_start, park_end, charging_timesteps):
    """Shorten the parking windows of `street` events as GeoLIS does.

    GeoLIS places `street` events with a shortened parking window
    (`use_case_helpers.park_time_limitation`), but `ev_event` keeps the
    full parking time. With the full windows a street location holds more
    cars at once than it has charging points. In every day the stop
    overlaps the window of :data:`STREET_PARK_LIMIT_WINDOW`, the overlap
    beyond `max(limit, charging time)` is cut; the shortened window starts
    at `park_start` as in GeoLIS.

    The charging time is rounded up, so the charging always fits the
    shortened window (GeoLIS rounds the result down). This does not
    change the occupancy of the delivered placement.

    Parameters
    ----------
    park_start, park_end : numpy.ndarray
        Half-open parking window `[park_start, park_end)` in timesteps
    charging_timesteps : numpy.ndarray
        Charging time at nominal power, in timesteps

    Returns
    -------
    numpy.ndarray
        Shortened `park_end`, int64
    """
    start = np.asarray(park_start, dtype="int64")
    end = np.asarray(park_end, dtype="int64")
    if len(start) == 0:
        return end
    keep = np.maximum(np.ceil(charging_timesteps), STREET_PARK_LIMIT_TIMESTEPS)
    window_start, window_end = STREET_PARK_LIMIT_WINDOW
    first_day = start // TIMESTEPS_PER_DAY
    n_days = int(((end - 1) // TIMESTEPS_PER_DAY - first_day).max()) + 1
    cut = np.zeros(len(start))
    for day in range(n_days):
        offset = (first_day + day) * TIMESTEPS_PER_DAY
        overlap = np.clip(
            np.minimum(end, offset + window_end)
            - np.maximum(start, offset + window_start),
            0,
            None,
        )
        cut += np.maximum(0, overlap - keep)
    return end - cut.astype("int64")


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
