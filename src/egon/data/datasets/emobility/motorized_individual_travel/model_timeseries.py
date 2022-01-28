"""
Generate timeseries for eTraGo and pypsa-eur-sec

Call order
  * generate_model_data_eGon2035() / generate_model_data_eGon100RE()
    * generate_model_data()
      * generate_model_data_grid_district()
        * load_evs_trips()
        * data_preprocessing()
        * generate_load_time_series()
        * generate_dsm_profile()
        * write_model_data_to_db()
"""

import os
import json
from pathlib import Path
import pandas as pd
import numpy as np
from collections import Counter

from egon.data import db
from egon.data.datasets.scenario_parameters import (
    get_sector_parameters,
)
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (
    EgonEvPool,
    EgonEvTrip,
    EgonEvMvGridDistrict
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    DATASET_CFG,
    WORKING_DIR,
    read_simbev_metadata_file,
    reduce_mem_usage
)


def data_preprocessing(
    scenario_data: pd.DataFrame,
    ev_data_df: pd.DataFrame,
    flex_dict: dict,
) -> pd.DataFrame:
    """Filter SimBEV data to match region requirements. Duplicates profiles
    if necessary. Pre-calculates necessary parameters for the load time series.

    Parameters
    ----------
    scenario_data : pd.Dataframe
        EV per grid district
    ev_data_df : pd.Dataframe
        Trip data
    flex_dict : dict
        Shares of charging infrastructure where charging can take place

    Returns
    -------
    pd.Dataframe
        Trip data
    """
    # count profiles to respect profiles which are used multiple times
    count_profiles = Counter(scenario_data.ev_id)  # type: dict

    max_duplicates = max(count_profiles.values())

    # get ev data for given profiles
    ev_data_df = ev_data_df.loc[ev_data_df.ev_id.isin(
        scenario_data.ev_id.unique())
    ]

    # drop faulty data
    ev_data_df = ev_data_df.loc[
        ev_data_df.park_start < ev_data_df.park_end
    ]

    if max_duplicates >= 2:
        # duplicate profiles if necessary
        temp = ev_data_df.copy()

        for count in range(2, max_duplicates + 1):
            duplicates = [
                key for key, val in count_profiles.items() if val >= count
            ]

            duplicates_df = temp.loc[temp.ev_id.isin(duplicates)]

            duplicates_df = duplicates_df.assign(
                ev_id=duplicates_df.ev_id.astype(str) + f"_{count}"
            )

            ev_data_df = ev_data_df.append(duplicates_df)

    # calculate time necessary to fulfill the charging demand and brutto
    # charging capacity in mva
    ev_data_df = ev_data_df.assign(
        charging_capacity_grid_MW=(
            ev_data_df.charging_capacity_grid / 10 ** 3
        ),
        minimum_charging_time=(
            ev_data_df.charging_demand /
            ev_data_df.charging_capacity_nominal * 4
        ),
        location=ev_data_df.location.str.replace("/", "_"),
    )

    # calculate charging capacity for last timestep
    (
        full_timesteps,
        last_timestep_share,
    ) = ev_data_df.minimum_charging_time.divmod(1)

    full_timesteps = full_timesteps.astype(int)

    ev_data_df = ev_data_df.assign(
        full_timesteps=full_timesteps,
        last_timestep_share=last_timestep_share,
        last_timestep_charging_capacity_grid_MW=(
            last_timestep_share * ev_data_df.charging_capacity_grid_MW
        ),
        charge_end=ev_data_df.park_start + full_timesteps,
        last_timestep=ev_data_df.park_start + full_timesteps + 1,
    )

    # calculate flexible charging capacity
    flex_share = ev_data_df.location.map(flex_dict)

    ev_data_df = ev_data_df.assign(
        flex_charging_capacity_grid_MW=(
            ev_data_df.charging_capacity_grid_MW * flex_share
        ),
        flex_last_timestep_charging_capacity_grid_MW=(
            ev_data_df.last_timestep_charging_capacity_grid_MW * flex_share
        ),
    )

    if DATASET_CFG["model_timeseries"]["reduce_memory"]:
        return reduce_mem_usage(ev_data_df)

    return ev_data_df


def generate_dsm_profile(
    start_date: str,
    end_date: str,
    restriction_time: int,
    min_soc: float
) -> pd.DataFrame:
    """Generate DSM profile (min SoC for each timestep)

    Parameters
    ----------
    start_date : str
        Start date used for timeindex
    end_date : str
        End date used for timeindex
    restriction_time : int
        Hour of day to set `min_soc` for
    min_soc : float
        Minimum state of charge

    Returns
    -------
    pd.DataFrame
        DSM profile
    """
    # Calc no of timesteps for year
    timestep_count = len(pd.date_range(
        start=f"{start_date} 00:00:00", end=f"{end_date} 23:00:00", freq='H'
    ))
    dsm_profile_week = np.zeros((24 * 7,))
    dsm_profile_week[(np.arange(0, 7, 1) * 24 + restriction_time)] = min_soc
    weeks, rest = divmod(timestep_count, len(dsm_profile_week))
    dsm_profile = np.concatenate(
        (np.tile(dsm_profile_week, weeks), dsm_profile_week[0:rest])
    )

    return pd.DataFrame({"min_soc": dsm_profile})


def generate_load_time_series(
    ev_data_df: pd.DataFrame,
    start_date: str,
    timestep: str
) -> pd.DataFrame:
    """Calculate the load time series from the given trip data. A dumb
    charging strategy is assumed where each EV starts charging immediately
    after plugging it in. Simultaneously the flexible charging capacity is
    calculated.

    Parameters
    ----------
    ev_data_df : pd.DataFrame
        Full trip data
    start_date : str
        Start date used for timeindex
    timestep : str
        Interval used for timeindex

    Returns
    -------
    pd.DataFrame
        time series of the load and the flex potential
    """
    # instantiate timeindex
    timeindex = pd.date_range(
        start_date,
        periods=ev_data_df.last_timestep.max() + 1,
        freq=timestep,
    )

    load_time_series_df = pd.DataFrame(
        data=0.0,
        index=timeindex,
        columns=["load_time_series", "flex_time_series"],
    )

    load_time_series_array = np.zeros(len(load_time_series_df))
    flex_time_series_array = load_time_series_array.copy()
    simultaneous_plugged_in_charging_capacity = load_time_series_array.copy()

    columns = [
        "park_start",
        "park_end",
        "charge_end",
        "charging_capacity_grid_MW",
        "last_timestep",
        "last_timestep_charging_capacity_grid_MW",
        "flex_charging_capacity_grid_MW",
        "flex_last_timestep_charging_capacity_grid_MW",
    ]

    # iterate over charging events
    for (
        _,
        start,
        park_end,
        end,
        cap,
        last_ts,
        last_ts_cap,
        flex_cap,
        flex_last_ts_cap,
    ) in ev_data_df[columns].itertuples():
        load_time_series_array[start:end] += cap
        load_time_series_array[last_ts] += last_ts_cap

        flex_time_series_array[start:end] += flex_cap
        flex_time_series_array[last_ts] += flex_last_ts_cap

        simultaneous_plugged_in_charging_capacity[start:park_end] += cap

    load_time_series_df = load_time_series_df.assign(
        load_time_series=load_time_series_array,
        flex_time_series=flex_time_series_array,
        simultaneous_plugged_in_charging_capacity=(
            simultaneous_plugged_in_charging_capacity
        ),
    )

    np.testing.assert_almost_equal(
        load_time_series_df.load_time_series.sum() / 4,
        ev_data_df.charging_demand.sum()
        / 1000
        / (
            ev_data_df.charging_capacity_nominal
            / ev_data_df.charging_capacity_grid
        ).mean(),
        decimal=0, #4,
    )

    if DATASET_CFG["model_timeseries"]["reduce_memory"]:
        return reduce_mem_usage(load_time_series_df)
    return load_time_series_df


def generate_static_params(
    ev_data_df: pd.DataFrame, load_time_series_df: pd.DataFrame,
) -> dict:
    """Calculate static parameters from trip data.

    Parameters
    ----------
    ev_data_df : pd.DataFrame
        Fill trip data

    Returns
    -------
    dict
        Static parameters
    """
    max_df = (
        ev_data_df[["ev_id", "bat_cap", "charging_capacity_grid_MW"]]
        .groupby("ev_id")
        .max()
    )

    static_params_dict = {
        "store_ev_battery.e_nom_MWh": float(max_df.bat_cap.sum() / 10 ** 3),
        "link_bev_charger.p_nom_MW": float(
            load_time_series_df.simultaneous_plugged_in_charging_capacity.max()
        ),
        "store_ev_battery.e_max_pu": 1,
    }

    return static_params_dict


def load_evs_trips(
    evs_ids: list, charging_events_only: bool = True
) -> pd.DataFrame:
    """Load trips for EVs

    Parameters
    ----------
    evs_ids : list of int
        IDs of EV to load the trips for
    charging_events_only : bool
        Load only events where charging takes place

    Returns
    -------
    pd.DataFrame
        Trip data
    """
    # Select only charigung events
    if charging_events_only is True:
        charging_condition = EgonEvTrip.charging_demand > 0
    else:
        charging_condition = EgonEvTrip.charging_demand >= 0

    with db.session_scope() as session:
        query = session.query(
            EgonEvTrip.egon_ev_pool_ev_id.label("ev_id"),
            EgonEvTrip.location,
            EgonEvTrip.charging_capacity_nominal,
            EgonEvTrip.charging_capacity_grid,
            EgonEvTrip.charging_capacity_battery,
            EgonEvTrip.soc_start,
            EgonEvTrip.soc_end,
            EgonEvTrip.charging_demand,
            EgonEvTrip.park_start,
            EgonEvTrip.park_end,
            EgonEvTrip.drive_start,
            EgonEvTrip.drive_end,
            EgonEvTrip.consumption,
            EgonEvPool.type
        ).join(
            EgonEvPool,
            EgonEvPool.ev_id == EgonEvTrip.egon_ev_pool_ev_id
        ).filter(
            EgonEvTrip.egon_ev_pool_ev_id.in_(evs_ids)
        ).filter(
            charging_condition
        ).order_by(
            EgonEvTrip.egon_ev_pool_ev_id, EgonEvTrip.simbev_event_id
        )
    trip_data = pd.read_sql(
        query.statement,
        query.session.bind,
        index_col=None
    ).astype({
        'ev_id': 'int',
        'park_start': 'int',
        'park_end': 'int',
        'drive_start': 'int',
        'drive_end': 'int'
    })
    return trip_data


def write_model_data_to_db(#export_results_grid_district(
    static_params_dict: dict,
    load_time_series_df: pd.DataFrame,
    dsm_profile_df: pd.DataFrame,
    bus_id: int,
    scenario_name: str,
) -> None:
    """Export all results for grid district as CSVs and add metadata JSON

    Parameters
    ----------
    static_params_dict : dict
        Static model params
    load_time_series_df : pd.DataFrame
        Load time series for grid district
    dsm_profile_df : pd.DataFrame
        DSM profile (min SoC for each timestep)
    bus_id : int
        ID of grid district
    scenario_name : str
        Scenario name

    Returns
    -------
    None
    """

    print("  Writing model timeseries...")
    load_time_series_df = load_time_series_df.assign(
        ev_availability=(
            load_time_series_df.flex_time_series
            / static_params_dict["link_bev_charger.p_nom_MW"]
        )
    )

    # Resample to 1h
    hourly_load_time_series_df = load_time_series_df.resample("1H").mean()

    # Align load and DSM timeseries
    if len(hourly_load_time_series_df) >= len(dsm_profile_df):
        hourly_load_time_series_df = hourly_load_time_series_df.iloc[
            : len(dsm_profile_df)
        ]
    else:
        dsm_profile_df = dsm_profile_df.iloc[: len(hourly_load_time_series_df)]
    dsm_profile_df.index = hourly_load_time_series_df.index

    # Write to eTraGo tables
    # PLACEHOLDER

    # Export to working dir if requested
    if DATASET_CFG["model_timeseries"]["export_results_to_csv"]:
        results_dir = WORKING_DIR / Path("results", scenario_name, str(bus_id))
        results_dir.mkdir(exist_ok=True, parents=True)

        hourly_load_time_series_df[["load_time_series"]].to_csv(
            results_dir / "ev_load_time_series.csv"
        )
        hourly_load_time_series_df[["ev_availability"]].to_csv(
            results_dir / "ev_availability.csv"
        )
        dsm_profile_df.to_csv(results_dir / "ev_dsm_profile.csv")

        static_params_dict[
            "load_land_transport_ev.p_set_MW"
        ] = "ev_load_time_series.csv"
        static_params_dict["link_bev_charger.p_max_pu"] = "ev_availability.csv"
        static_params_dict["store_ev_battery.e_min_pu"] = "ev_dsm_profile.csv"

        file = results_dir / "ev_static_params.json"

        with open(file, "w") as f:
            json.dump(static_params_dict, f, indent=4)


def generate_model_data_grid_district(
    evs_grid_district: pd.DataFrame,
    scenario_variation_parameters: dict,
    bat_cap_dict: dict,
    run_config: pd.DataFrame
) -> tuple:
    """Generates timeseries from simBEV trip data for MV grid district

    Parameters
    ----------
    evs_grid_district : pd.DataFrame
        EV data for grid district
    scenario_variation_parameters : dict
        Scenario variation parameters
    bat_cap_dict : dict
        Battery capacity per EV type
    run_config : pd.DataFrame
        simBEV metadata: run config

    Returns
    -------
    pd.DataFrame
        Model data for grid district
    """

    # Load trip data
    print("  Loading trips...")
    trip_data = load_evs_trips(
        evs_ids=evs_grid_district.ev_id.unique(),
        charging_events_only=True
    )

    print("  Preprocessing data...")
    # Assign battery capacity to trip data
    trip_data["bat_cap"] = trip_data.type.apply(lambda _: bat_cap_dict[_])
    trip_data.drop(columns=["type"], inplace=True)

    # TODO: change this when SimBEV issue #33 is solved
    #  https://github.com/rl-institut/simbev/issues/33
    flex_dict = scenario_variation_parameters["flex_share"]

    # Preprocess trip data
    trip_data = data_preprocessing(
        evs_grid_district, trip_data, flex_dict
    )

    # Generate load timeseries
    print("  Generating load timeseries...")
    load_ts = generate_load_time_series(
        ev_data_df=trip_data,
        start_date=run_config.start_date,
        timestep=f"{int(run_config.stepsize)}Min"
    )

    # Generate static paras
    static_params = generate_static_params(trip_data, load_ts)

    # generate DSM profile
    print("  Generating DSM profile...")
    model_parameters = scenario_variation_parameters["model_parameters"]
    dsm_profile = generate_dsm_profile(
        start_date=run_config.start_date,
        end_date=run_config.end_date,
        restriction_time=model_parameters["restriction_time"],
        min_soc=model_parameters["min_soc"]
    )

    return static_params, load_ts, dsm_profile


def generate_model_data(scenario_name: str):
    """Generates timeseries from simBEV trip data for all MV grid districts

    Parameters
    ----------
    scenario_name : str
        Scenario name
    """

    # Create dir for results, if it does not exist
    result_dir = WORKING_DIR / Path("results")
    if not os.path.exists(result_dir):
        os.mkdir(result_dir)

    # Get scenario variation name
    scenario_var_name = DATASET_CFG["scenario"]["variation"][scenario_name]

    print(
        f"SCENARIO: {scenario_name}, SCENARIO VARIATION: {scenario_var_name}"
    )

    # Load scenario params for scenario and scenario variation
    scenario_variation_parameters = get_sector_parameters(
        "mobility", scenario=scenario_name
    )["motorized_individual_travel"][scenario_var_name]

    # Get substations
    with db.session_scope() as session:
        query = session.query(
            EgonEvMvGridDistrict.bus_id,
            EgonEvMvGridDistrict.egon_ev_pool_ev_id.label("ev_id"),
        ).filter(
            EgonEvMvGridDistrict.scenario == scenario_name
        ).filter(
            EgonEvMvGridDistrict.scenario_variation == scenario_var_name
        ).filter(
            EgonEvMvGridDistrict.egon_ev_pool_ev_id.isnot(None)
        )
    evs_grid_district = pd.read_sql(
        query.statement,
        query.session.bind,
        index_col=None
    ).astype({"ev_id": "int"})

    mvgd_bus_ids = evs_grid_district.bus_id.unique()
    print(
        f"{len(evs_grid_district)} EV loaded "
        f"({len(evs_grid_district.ev_id.unique())} unique) in "
        f"{len(mvgd_bus_ids)} grid districts."
    )

    # Get run metadata
    meta_tech_data = read_simbev_metadata_file(scenario_name, "tech_data")
    meta_run_config = read_simbev_metadata_file(scenario_name,
                                                "config").loc["basic"]

    # Generate timeseries for each MVGD
    print("GENERATE MODEL DATA...")
    for bus_id in mvgd_bus_ids:
        print(f"Processing grid district {bus_id}...")
        static_params, load_ts, dsm_profile = \
            generate_model_data_grid_district(
                evs_grid_district=evs_grid_district[
                    evs_grid_district.bus_id == bus_id
                    ],
                scenario_variation_parameters=scenario_variation_parameters,
                bat_cap_dict=meta_tech_data.battery_capacity.to_dict(),
                run_config=meta_run_config
            )
        write_model_data_to_db(
            static_params, load_ts, dsm_profile, bus_id, scenario_name
        )


def generate_model_data_eGon2035():
    """Generates timeseries for eGon2035 scenario"""
    generate_model_data(scenario_name="eGon2035")


def generate_model_data_eGon100RE():
    """Generates timeseries for eGon100RE scenario"""
    generate_model_data(scenario_name="eGon100RE")
