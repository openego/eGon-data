"""
Generate timeseries for eTraGo and pypsa-eur-sec

Call order
  * generate_model_data_eGon2035() / generate_model_data_eGon100RE()
    * generate_model_data()
      * generate_model_data_grid_district()
        * load_evs_trips()
        * data_preprocessing()
        * generate_load_time_series()
        * write_model_data_to_db()

Notes
-----
# TODO REWORK
Share of EV with access to private charging infrastructure (`flex_share`) for
use cases work and home are not supported by simBEV v0.1.2 and are applied here
(after simulation). Applying those fixed shares post-simulation introduces
small errors compared to application during simBEV's trip generation.

Values (cf. `flex_share` in scenario parameters
:func:`egon.data.datasets.scenario_parameters.parameters.mobility`) were
linearly extrapolated based upon
https://nationale-leitstelle.de/wp-content/pdf/broschuere-lis-2025-2030-final.pdf
(p.92):
* eGon2035: home=0.8, work=1.0
* eGon100RE: home=1.0, work=1.0
"""

from collections import Counter
from pathlib import Path
import datetime as dt
import json

from sqlalchemy.sql import func
import numpy as np
import pandas as pd

from egon.data import db
from egon.data.datasets.emobility.motorized_individual_travel.db_classes import (  # noqa: E501
    EgonEvMvGridDistrict,
    EgonEvPool,
    EgonEvTrip,
)
from egon.data.datasets.emobility.motorized_individual_travel.helpers import (
    DATASET_CFG,
    MVGD_MIN_COUNT,
    WORKING_DIR,
    read_simbev_metadata_file,
    reduce_mem_usage,
)
from egon.data.datasets.etrago_setup import (
    EgonPfHvBus,
    EgonPfHvLink,
    EgonPfHvLinkTimeseries,
    EgonPfHvLoad,
    EgonPfHvLoadTimeseries,
    EgonPfHvStore,
    EgonPfHvStoreTimeseries,
)
from egon.data.datasets.mv_grid_districts import MvGridDistricts


def data_preprocessing(
    scenario_data: pd.DataFrame, ev_data_df: pd.DataFrame
) -> pd.DataFrame:
    """Filter SimBEV data to match region requirements. Duplicates profiles
    if necessary. Pre-calculates necessary parameters for the load time series.

    Parameters
    ----------
    scenario_data : pd.Dataframe
        EV per grid district
    ev_data_df : pd.Dataframe
        Trip data

    Returns
    -------
    pd.Dataframe
        Trip data
    """
    # get ev data for given profiles
    ev_data_df = ev_data_df.loc[
        ev_data_df.ev_id.isin(scenario_data.ev_id.unique())
    ]

    # drop faulty data
    ev_data_df = ev_data_df.loc[ev_data_df.park_start <= ev_data_df.park_end]

    # calculate time necessary to fulfill the charging demand and brutto
    # charging capacity in MVA
    ev_data_df = ev_data_df.assign(
        charging_capacity_grid_MW=(
            ev_data_df.charging_capacity_grid / 10**3
        ),
        minimum_charging_time=(
            ev_data_df.charging_demand
            / ev_data_df.charging_capacity_nominal
            * 4
        ),
        location=ev_data_df.location.str.replace("/", "_"),
    )

    # fix driving events
    ev_data_df.minimum_charging_time.fillna(0, inplace=True)

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
        last_timestep=ev_data_df.park_start + full_timesteps,
    )

    # Calculate flexible charging capacity:
    # only for private charging facilities at home and work
    mask_work = (ev_data_df.location == "0_work") & (
        ev_data_df.use_case == "work"
    )
    mask_home = (ev_data_df.location == "6_home") & (
        ev_data_df.use_case == "home"
    )

    ev_data_df["flex_charging_capacity_grid_MW"] = 0
    ev_data_df.loc[
        mask_work | mask_home, "flex_charging_capacity_grid_MW"
    ] = ev_data_df.loc[mask_work | mask_home, "charging_capacity_grid_MW"]

    ev_data_df["flex_last_timestep_charging_capacity_grid_MW"] = 0
    ev_data_df.loc[
        mask_work | mask_home, "flex_last_timestep_charging_capacity_grid_MW"
    ] = ev_data_df.loc[
        mask_work | mask_home, "last_timestep_charging_capacity_grid_MW"
    ]

    # Check length of timeseries
    if len(ev_data_df.loc[ev_data_df.last_timestep > 35040]) > 0:
        print("    Warning: Trip data exceeds 1 year and is cropped.")
        # Correct last TS
        ev_data_df.loc[
            ev_data_df.last_timestep > 35040, "last_timestep"
        ] = 35040

    if DATASET_CFG["model_timeseries"]["reduce_memory"]:
        return reduce_mem_usage(ev_data_df)

    return ev_data_df


def generate_load_time_series(
    ev_data_df: pd.DataFrame,
    run_config: pd.DataFrame,
    scenario_data: pd.DataFrame,
) -> pd.DataFrame:
    """Calculate the load time series from the given trip data. A dumb
    charging strategy is assumed where each EV starts charging immediately
    after plugging it in. Simultaneously the flexible charging capacity is
    calculated.

    Parameters
    ----------
    ev_data_df : pd.DataFrame
        Full trip data
    run_config : pd.DataFrame
        simBEV metadata: run config
    scenario_data : pd.Dataframe
        EV per grid district

    Returns
    -------
    pd.DataFrame
        time series of the load and the flex potential
    """
    # Get duplicates dict
    profile_counter = Counter(scenario_data.ev_id)

    # instantiate timeindex
    timeindex = pd.date_range(
        start=dt.datetime.fromisoformat(f"{run_config.start_date} 00:00:00"),
        end=dt.datetime.fromisoformat(f"{run_config.end_date} 23:45:00")
        + dt.timedelta(minutes=int(run_config.stepsize)),
        freq=f"{int(run_config.stepsize)}Min",
    )

    load_time_series_df = pd.DataFrame(
        data=0.0,
        index=timeindex,
        columns=["load_time_series", "flex_time_series"],
    )

    load_time_series_array = np.zeros(len(load_time_series_df))
    flex_time_series_array = load_time_series_array.copy()
    simultaneous_plugged_in_charging_capacity = load_time_series_array.copy()
    simultaneous_plugged_in_charging_capacity_flex = (
        load_time_series_array.copy()
    )
    soc_min_absolute = load_time_series_array.copy()
    soc_max_absolute = load_time_series_array.copy()
    driving_load_time_series_array = load_time_series_array.copy()

    columns = [
        "ev_id",
        "drive_start",
        "drive_end",
        "park_start",
        "park_end",
        "charge_end",
        "charging_capacity_grid_MW",
        "last_timestep",
        "last_timestep_charging_capacity_grid_MW",
        "flex_charging_capacity_grid_MW",
        "flex_last_timestep_charging_capacity_grid_MW",
        "soc_start",
        "soc_end",
        "bat_cap",
        "location",
        "consumption",
    ]

    # iterate over charging events
    for (
        _,
        ev_id,
        drive_start,
        drive_end,
        start,
        park_end,
        end,
        cap,
        last_ts,
        last_ts_cap,
        flex_cap,
        flex_last_ts_cap,
        soc_start,
        soc_end,
        bat_cap,
        location,
        consumption,
    ) in ev_data_df[columns].itertuples():
        ev_count = profile_counter[ev_id]

        load_time_series_array[start:end] += cap * ev_count
        load_time_series_array[last_ts] += last_ts_cap * ev_count

        flex_time_series_array[start:end] += flex_cap * ev_count
        flex_time_series_array[last_ts] += flex_last_ts_cap * ev_count

        simultaneous_plugged_in_charging_capacity[start : park_end + 1] += (
            cap * ev_count
        )
        simultaneous_plugged_in_charging_capacity_flex[
            start : park_end + 1
        ] += (flex_cap * ev_count)

        # ====================================================
        # min and max SoC constraints of aggregated EV battery
        # ====================================================
        # (I) Preserve SoC while driving
        if location == "driving":
            # Full band while driving
            # soc_min_absolute[drive_start:drive_end+1] +=
            # soc_end * bat_cap * ev_count
            #
            # soc_max_absolute[drive_start:drive_end+1] +=
            # soc_start * bat_cap * ev_count

            # Real band (decrease SoC while driving)
            soc_min_absolute[drive_start : drive_end + 1] += (
                np.linspace(soc_start, soc_end, drive_end - drive_start + 2)[
                    1:
                ]
                * bat_cap
                * ev_count
            )
            soc_max_absolute[drive_start : drive_end + 1] += (
                np.linspace(soc_start, soc_end, drive_end - drive_start + 2)[
                    1:
                ]
                * bat_cap
                * ev_count
            )

            # Equal distribution of driving load
            if soc_start > soc_end:  # reqd. for PHEV
                driving_load_time_series_array[
                    drive_start : drive_end + 1
                ] += (consumption * ev_count) / (drive_end - drive_start + 1)

        # (II) Fix SoC bounds while parking w/o charging
        elif soc_start == soc_end:
            soc_min_absolute[start : park_end + 1] += (
                soc_start * bat_cap * ev_count
            )
            soc_max_absolute[start : park_end + 1] += (
                soc_end * bat_cap * ev_count
            )

        # (III) Set SoC bounds at start and end of parking while charging
        # for flexible and non-flexible events
        elif soc_start < soc_end:
            if flex_cap > 0:
                # * "flex" (private charging only, band: SoC_min..SoC_max)
                soc_min_absolute[start : park_end + 1] += (
                    soc_start * bat_cap * ev_count
                )
                soc_max_absolute[start : park_end + 1] += (
                    soc_end * bat_cap * ev_count
                )

                # * "flex+" (private charging only, band: 0..1)
                #   (IF USED: add elif with flex scenario)
                # soc_min_absolute[start] += soc_start * bat_cap * ev_count
                # soc_max_absolute[start] += soc_start * bat_cap * ev_count
                # soc_min_absolute[park_end] += soc_end * bat_cap * ev_count
                # soc_max_absolute[park_end] += soc_end * bat_cap * ev_count

            # * Set SoC bounds for non-flexible charging (increase SoC while
            #   charging)
            # (SKIP THIS PART for "flex++" (private+public charging))
            elif flex_cap == 0:
                soc_min_absolute[start : park_end + 1] += (
                    np.linspace(soc_start, soc_end, park_end - start + 1)
                    * bat_cap
                    * ev_count
                )
                soc_max_absolute[start : park_end + 1] += (
                    np.linspace(soc_start, soc_end, park_end - start + 1)
                    * bat_cap
                    * ev_count
                )

    # Build timeseries
    load_time_series_df = load_time_series_df.assign(
        load_time_series=load_time_series_array,
        flex_time_series=flex_time_series_array,
        simultaneous_plugged_in_charging_capacity=(
            simultaneous_plugged_in_charging_capacity
        ),
        simultaneous_plugged_in_charging_capacity_flex=(
            simultaneous_plugged_in_charging_capacity_flex
        ),
        soc_min_absolute=(soc_min_absolute / 1e3),
        soc_max_absolute=(soc_max_absolute / 1e3),
        driving_load_time_series=driving_load_time_series_array / 1e3,
    )

    # validate load timeseries
    np.testing.assert_almost_equal(
        load_time_series_df.load_time_series.sum() / 4,
        (
            ev_data_df.ev_id.apply(lambda _: profile_counter[_])
            * ev_data_df.charging_demand
        ).sum()
        / 1000
        / float(run_config.eta_cp),
        decimal=-1,
    )

    if DATASET_CFG["model_timeseries"]["reduce_memory"]:
        return reduce_mem_usage(load_time_series_df)
    return load_time_series_df


def generate_static_params(
    ev_data_df: pd.DataFrame,
    load_time_series_df: pd.DataFrame,
    evs_grid_district_df: pd.DataFrame,
) -> dict:
    """Calculate static parameters from trip data.

    * cumulative initial SoC
    * cumulative battery capacity
    * simultaneous plugged in charging capacity

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

    # Get EV duplicates dict and weight battery capacity
    max_df["bat_cap"] = max_df.bat_cap.mul(
        pd.Series(Counter(evs_grid_district_df.ev_id))
    )

    static_params_dict = {
        "store_ev_battery.e_nom_MWh": float(max_df.bat_cap.sum() / 1e3),
        "link_bev_charger.p_nom_MW": float(
            load_time_series_df.simultaneous_plugged_in_charging_capacity.max()
        ),
    }

    return static_params_dict


def load_evs_trips(
    scenario_name: str,
    evs_ids: list,
    charging_events_only: bool = False,
    flex_only_at_charging_events: bool = True,
) -> pd.DataFrame:
    """Load trips for EVs

    Parameters
    ----------
    scenario_name : str
        Scenario name
    evs_ids : list of int
        IDs of EV to load the trips for
    charging_events_only : bool
        Load only events where charging takes place
    flex_only_at_charging_events : bool
        Flexibility only at charging events. If False, flexibility is provided
        by plugged-in EVs even if no charging takes place.

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
        query = (
            session.query(
                EgonEvTrip.egon_ev_pool_ev_id.label("ev_id"),
                EgonEvTrip.location,
                EgonEvTrip.use_case,
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
                EgonEvPool.type,
            )
            .join(
                EgonEvPool, EgonEvPool.ev_id == EgonEvTrip.egon_ev_pool_ev_id
            )
            .filter(EgonEvTrip.egon_ev_pool_ev_id.in_(evs_ids))
            .filter(EgonEvTrip.scenario == scenario_name)
            .filter(EgonEvPool.scenario == scenario_name)
            .filter(charging_condition)
            .order_by(
                EgonEvTrip.egon_ev_pool_ev_id, EgonEvTrip.simbev_event_id
            )
        )

    trip_data = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).astype(
        {
            "ev_id": "int",
            "park_start": "int",
            "park_end": "int",
            "drive_start": "int",
            "drive_end": "int",
        }
    )

    if flex_only_at_charging_events is True:
        # ASSUMPTION: set charging cap 0 where there's no demand
        # (discard other plugged-in times)
        mask = trip_data.charging_demand == 0
        trip_data.loc[mask, "charging_capacity_nominal"] = 0
        trip_data.loc[mask, "charging_capacity_grid"] = 0
        trip_data.loc[mask, "charging_capacity_battery"] = 0

    return trip_data


def write_model_data_to_db(
    static_params_dict: dict,
    load_time_series_df: pd.DataFrame,
    bus_id: int,
    scenario_name: str,
    run_config: pd.DataFrame,
    bat_cap: pd.DataFrame,
) -> None:
    """Write all results for grid district to database

    Parameters
    ----------
    static_params_dict : dict
        Static model params
    load_time_series_df : pd.DataFrame
        Load time series for grid district
    bus_id : int
        ID of grid district
    scenario_name : str
        Scenario name
    run_config : pd.DataFrame
        simBEV metadata: run config
    bat_cap : pd.DataFrame
        Battery capacities per EV type

    Returns
    -------
    None
    """

    def calc_initial_ev_soc(bus_id: int, scenario_name: str) -> pd.DataFrame:
        """Calculate an average initial state of charge for EVs in MV grid
        district.

        This is done by weighting the initial SoCs at timestep=0 with EV count
        and battery capacity for each EV type.
        """
        with db.session_scope() as session:
            query_ev_soc = (
                session.query(
                    EgonEvPool.type,
                    func.count(EgonEvTrip.egon_ev_pool_ev_id).label(
                        "ev_count"
                    ),
                    func.avg(EgonEvTrip.soc_start).label("ev_soc_start"),
                )
                .select_from(EgonEvTrip)
                .join(
                    EgonEvPool,
                    EgonEvPool.ev_id == EgonEvTrip.egon_ev_pool_ev_id,
                )
                .join(
                    EgonEvMvGridDistrict,
                    EgonEvMvGridDistrict.egon_ev_pool_ev_id
                    == EgonEvTrip.egon_ev_pool_ev_id,
                )
                .filter(
                    EgonEvTrip.scenario == scenario_name,
                    EgonEvPool.scenario == scenario_name,
                    EgonEvMvGridDistrict.scenario == scenario_name,
                    EgonEvMvGridDistrict.bus_id == bus_id,
                    EgonEvTrip.simbev_event_id == 0,
                )
                .group_by(EgonEvPool.type)
            )

        initial_soc_per_ev_type = pd.read_sql(
            query_ev_soc.statement, query_ev_soc.session.bind, index_col="type"
        )

        initial_soc_per_ev_type[
            "battery_capacity_sum"
        ] = initial_soc_per_ev_type.ev_count.multiply(bat_cap)
        initial_soc_per_ev_type[
            "ev_soc_start_abs"
        ] = initial_soc_per_ev_type.battery_capacity_sum.multiply(
            initial_soc_per_ev_type.ev_soc_start
        )

        return (
            initial_soc_per_ev_type.ev_soc_start_abs.sum()
            / initial_soc_per_ev_type.battery_capacity_sum.sum()
        )

    def write_to_db(write_lowflex_model: bool) -> None:
        """Write model data to eTraGo tables"""

        @db.check_db_unique_violation
        def write_bus(scenario_name: str) -> int:
            # eMob MIT bus
            emob_bus_id = db.next_etrago_id("bus")
            with db.session_scope() as session:
                session.add(
                    EgonPfHvBus(
                        scn_name=scenario_name,
                        bus_id=emob_bus_id,
                        v_nom=1,
                        carrier="Li ion",
                        x=etrago_bus.x,
                        y=etrago_bus.y,
                        geom=etrago_bus.geom,
                    )
                )
            return emob_bus_id

        @db.check_db_unique_violation
        def write_link(scenario_name: str) -> None:
            # eMob MIT link [bus_el] -> [bus_ev]
            emob_link_id = db.next_etrago_id("link")
            with db.session_scope() as session:
                session.add(
                    EgonPfHvLink(
                        scn_name=scenario_name,
                        link_id=emob_link_id,
                        bus0=etrago_bus.bus_id,
                        bus1=emob_bus_id,
                        carrier="BEV charger",
                        efficiency=float(run_config.eta_cp),
                        p_nom=(
                            load_time_series_df.simultaneous_plugged_in_charging_capacity.max()  # noqa: E501
                        ),
                        p_nom_extendable=False,
                        p_nom_min=0,
                        p_nom_max=np.Inf,
                        p_min_pu=0,
                        p_max_pu=1,
                        # p_set_fixed=0,
                        capital_cost=0,
                        marginal_cost=0,
                        length=0,
                        terrain_factor=1,
                    )
                )
            with db.session_scope() as session:
                session.add(
                    EgonPfHvLinkTimeseries(
                        scn_name=scenario_name,
                        link_id=emob_link_id,
                        temp_id=1,
                        p_min_pu=None,
                        p_max_pu=(
                            hourly_load_time_series_df.ev_availability.to_list()  # noqa: E501
                        ),
                    )
                )

        @db.check_db_unique_violation
        def write_store(scenario_name: str) -> None:
            # eMob MIT store
            emob_store_id = db.next_etrago_id("store")
            with db.session_scope() as session:
                session.add(
                    EgonPfHvStore(
                        scn_name=scenario_name,
                        store_id=emob_store_id,
                        bus=emob_bus_id,
                        carrier="battery storage",
                        e_nom=static_params_dict["store_ev_battery.e_nom_MWh"],
                        e_nom_extendable=False,
                        e_nom_min=0,
                        e_nom_max=np.Inf,
                        e_min_pu=0,
                        e_max_pu=1,
                        e_initial=(
                            initial_soc_mean
                            * static_params_dict["store_ev_battery.e_nom_MWh"]
                        ),
                        e_cyclic=False,
                        sign=1,
                        standing_loss=0,
                    )
                )
            with db.session_scope() as session:
                session.add(
                    EgonPfHvStoreTimeseries(
                        scn_name=scenario_name,
                        store_id=emob_store_id,
                        temp_id=1,
                        e_min_pu=hourly_load_time_series_df.soc_min.to_list(),
                        e_max_pu=hourly_load_time_series_df.soc_max.to_list(),
                    )
                )

        @db.check_db_unique_violation
        def write_load(
            scenario_name: str, connection_bus_id: int, load_ts: list
        ) -> None:
            # eMob MIT load
            emob_load_id = db.next_etrago_id("load")
            with db.session_scope() as session:
                session.add(
                    EgonPfHvLoad(
                        scn_name=scenario_name,
                        load_id=emob_load_id,
                        bus=connection_bus_id,
                        carrier="land transport EV",
                        sign=-1,
                    )
                )
            with db.session_scope() as session:
                session.add(
                    EgonPfHvLoadTimeseries(
                        scn_name=scenario_name,
                        load_id=emob_load_id,
                        temp_id=1,
                        p_set=load_ts,
                    )
                )

        # Get eTraGo substation bus
        with db.session_scope() as session:
            query = session.query(
                EgonPfHvBus.scn_name,
                EgonPfHvBus.bus_id,
                EgonPfHvBus.x,
                EgonPfHvBus.y,
                EgonPfHvBus.geom,
            ).filter(
                EgonPfHvBus.scn_name == scenario_name,
                EgonPfHvBus.bus_id == bus_id,
                EgonPfHvBus.carrier == "AC",
            )
            etrago_bus = query.first()
            if etrago_bus is None:
                # TODO: raise exception here!
                print(
                    f"No AC bus found for scenario {scenario_name} "
                    f"with bus_id {bus_id} in table egon_etrago_bus!"
                )

        # Call DB writing functions for regular or lowflex scenario
        # * use corresponding scenario name as defined in datasets.yml
        # * no storage for lowflex scenario
        # * load timeseries:
        #   * regular (flex): use driving load
        #   * lowflex: use dumb charging load
        #   * status2019: also dumb charging

        if scenario_name=='status2019':
            write_load(
                scenario_name=scenario_name,
                connection_bus_id=etrago_bus.bus_id,
                load_ts=hourly_load_time_series_df.load_time_series.to_list(),
                )
        else:
            if write_lowflex_model is False:
                emob_bus_id = write_bus(scenario_name=scenario_name)
                write_link(scenario_name=scenario_name)
                write_store(scenario_name=scenario_name)
                write_load(
                    scenario_name=scenario_name,
                    connection_bus_id=emob_bus_id,
                    load_ts=(
                        hourly_load_time_series_df.driving_load_time_series.to_list()  # noqa: E501
                    ),
                )

            else:
                # Get lowflex scenario name
                lowflex_scenario_name = DATASET_CFG["scenario"]["lowflex"][
                    "names"
                ][scenario_name]
                write_load(
                    scenario_name=lowflex_scenario_name,
                    connection_bus_id=etrago_bus.bus_id,
                    load_ts=hourly_load_time_series_df.load_time_series.to_list(),
                )

    def write_to_file():
        """Write model data to file (for debugging purposes)"""
        results_dir = WORKING_DIR / Path("results", scenario_name, str(bus_id))
        results_dir.mkdir(exist_ok=True, parents=True)

        hourly_load_time_series_df[["load_time_series"]].to_csv(
            results_dir / "ev_load_time_series.csv"
        )
        hourly_load_time_series_df[["ev_availability"]].to_csv(
            results_dir / "ev_availability.csv"
        )
        hourly_load_time_series_df[["soc_min", "soc_max"]].to_csv(
            results_dir / "ev_dsm_profile.csv"
        )

        static_params_dict[
            "load_land_transport_ev.p_set_MW"
        ] = "ev_load_time_series.csv"
        static_params_dict["link_bev_charger.p_max_pu"] = "ev_availability.csv"
        static_params_dict["store_ev_battery.e_min_pu"] = "ev_dsm_profile.csv"
        static_params_dict["store_ev_battery.e_max_pu"] = "ev_dsm_profile.csv"

        file = results_dir / "ev_static_params.json"

        with open(file, "w") as f:
            json.dump(static_params_dict, f, indent=4)

    print("  Writing model timeseries...")
    load_time_series_df = load_time_series_df.assign(
        ev_availability=(
            load_time_series_df.simultaneous_plugged_in_charging_capacity
            / static_params_dict["link_bev_charger.p_nom_MW"]
        )
    )

    # Resample to 1h
    hourly_load_time_series_df = load_time_series_df.resample("1H").agg(
        {
            "load_time_series": np.mean,
            "flex_time_series": np.mean,
            "simultaneous_plugged_in_charging_capacity": np.mean,
            "simultaneous_plugged_in_charging_capacity_flex": np.mean,
            "soc_min_absolute": np.min,
            "soc_max_absolute": np.max,
            "ev_availability": np.mean,
            "driving_load_time_series": np.sum,
        }
    )

    # Create relative SoC timeseries
    hourly_load_time_series_df = hourly_load_time_series_df.assign(
        soc_min=hourly_load_time_series_df.soc_min_absolute.div(
            static_params_dict["store_ev_battery.e_nom_MWh"]
        ),
        soc_max=hourly_load_time_series_df.soc_max_absolute.div(
            static_params_dict["store_ev_battery.e_nom_MWh"]
        ),
    )
    hourly_load_time_series_df = hourly_load_time_series_df.assign(
        soc_delta_absolute=(
            hourly_load_time_series_df.soc_max_absolute
            - hourly_load_time_series_df.soc_min_absolute
        ),
        soc_delta=(
            hourly_load_time_series_df.soc_max
            - hourly_load_time_series_df.soc_min
        ),
    )

    # Crop hourly TS if needed
    hourly_load_time_series_df = hourly_load_time_series_df[:8760]

    # Create lowflex scenario?
    write_lowflex_model = DATASET_CFG["scenario"]["lowflex"][
        "create_lowflex_scenario"
    ]

    # Get initial average storage SoC
    initial_soc_mean = calc_initial_ev_soc(bus_id, scenario_name)

    # Write to database: regular and lowflex scenario
    write_to_db(write_lowflex_model=False)
    print("    Writing flex scenario...")
    if write_lowflex_model is True:
        print("    Writing lowflex scenario...")
        write_to_db(write_lowflex_model=True)

    # Export to working dir if requested
    if DATASET_CFG["model_timeseries"]["export_results_to_csv"]:
        write_to_file()


def delete_model_data_from_db():
    """Delete all eMob MIT data from eTraGo PF tables"""
    with db.session_scope() as session:
        # Buses
        session.query(EgonPfHvBus).filter(
            EgonPfHvBus.carrier == "Li ion"
        ).delete(synchronize_session=False)

        # Link TS
        subquery = (
            session.query(EgonPfHvLink.link_id)
            .filter(EgonPfHvLink.carrier == "BEV charger")
            .subquery()
        )

        session.query(EgonPfHvLinkTimeseries).filter(
            EgonPfHvLinkTimeseries.link_id.in_(subquery)
        ).delete(synchronize_session=False)
        # Links
        session.query(EgonPfHvLink).filter(
            EgonPfHvLink.carrier == "BEV charger"
        ).delete(synchronize_session=False)

        # Store TS
        subquery = (
            session.query(EgonPfHvStore.store_id)
            .filter(EgonPfHvStore.carrier == "battery storage")
            .subquery()
        )

        session.query(EgonPfHvStoreTimeseries).filter(
            EgonPfHvStoreTimeseries.store_id.in_(subquery)
        ).delete(synchronize_session=False)
        # Stores
        session.query(EgonPfHvStore).filter(
            EgonPfHvStore.carrier == "battery storage"
        ).delete(synchronize_session=False)

        # Load TS
        subquery = (
            session.query(EgonPfHvLoad.load_id)
            .filter(EgonPfHvLoad.carrier == "land transport EV")
            .subquery()
        )

        session.query(EgonPfHvLoadTimeseries).filter(
            EgonPfHvLoadTimeseries.load_id.in_(subquery)
        ).delete(synchronize_session=False)
        # Loads
        session.query(EgonPfHvLoad).filter(
            EgonPfHvLoad.carrier == "land transport EV"
        ).delete(synchronize_session=False)


def load_grid_district_ids() -> pd.Series:
    """Load bus IDs of all grid districts"""
    with db.session_scope() as session:
        query_mvgd = session.query(MvGridDistricts.bus_id)
    return pd.read_sql(
        query_mvgd.statement, query_mvgd.session.bind, index_col=None
    ).bus_id.sort_values()


def generate_model_data_grid_district(
    scenario_name: str,
    evs_grid_district: pd.DataFrame,
    bat_cap_dict: dict,
    run_config: pd.DataFrame,
) -> tuple:
    """Generates timeseries from simBEV trip data for MV grid district

    Parameters
    ----------
    scenario_name : str
        Scenario name
    evs_grid_district : pd.DataFrame
        EV data for grid district
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
        scenario_name=scenario_name,
        evs_ids=evs_grid_district.ev_id.unique(),
        charging_events_only=False,
        flex_only_at_charging_events=True,
    )

    print("  Preprocessing data...")
    # Assign battery capacity to trip data
    trip_data["bat_cap"] = trip_data.type.apply(lambda _: bat_cap_dict[_])
    trip_data.drop(columns=["type"], inplace=True)

    # Preprocess trip data
    trip_data = data_preprocessing(evs_grid_district, trip_data)

    # Generate load timeseries
    print("  Generating load timeseries...")
    load_ts = generate_load_time_series(
        ev_data_df=trip_data,
        run_config=run_config,
        scenario_data=evs_grid_district,
    )

    # Generate static params
    static_params = generate_static_params(
        trip_data, load_ts, evs_grid_district
    )

    return static_params, load_ts


def generate_model_data_bunch(scenario_name: str, bunch: range) -> None:
    """Generates timeseries from simBEV trip data for a bunch of MV grid
    districts.

    Parameters
    ----------
    scenario_name : str
        Scenario name
    bunch : list
        Bunch of grid districts to generate data for, e.g. [1,2,..,100].
        Note: `bunch` is NOT a list of grid districts but is used for slicing
        the ordered list (by bus_id) of grid districts! This is used for
        parallelization. See
        :meth:`egon.data.datasets.emobility.motorized_individual_travel.MotorizedIndividualTravel.generate_model_data_tasks`
    """
    # Get list of grid districts / substations for this bunch
    mvgd_bus_ids = load_grid_district_ids().iloc[bunch]

    # Get scenario variation name
    scenario_var_name = DATASET_CFG["scenario"]["variation"][scenario_name]

    print(
        f"SCENARIO: {scenario_name}, "
        f"SCENARIO VARIATION: {scenario_var_name}, "
        f"BUNCH: {bunch[0]}-{bunch[-1]}"
    )

    # Load scenario params for scenario and scenario variation
    # scenario_variation_parameters = get_sector_parameters(
    #    "mobility", scenario=scenario_name
    # )["motorized_individual_travel"][scenario_var_name]

    # Get substations
    with db.session_scope() as session:
        query = (
            session.query(
                EgonEvMvGridDistrict.bus_id,
                EgonEvMvGridDistrict.egon_ev_pool_ev_id.label("ev_id"),
            )
            .filter(EgonEvMvGridDistrict.scenario == scenario_name)
            .filter(
                EgonEvMvGridDistrict.scenario_variation == scenario_var_name
            )
            .filter(EgonEvMvGridDistrict.bus_id.in_(mvgd_bus_ids))
            .filter(EgonEvMvGridDistrict.egon_ev_pool_ev_id.isnot(None))
        )
    evs_grid_district = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).astype({"ev_id": "int"})

    mvgd_bus_ids = evs_grid_district.bus_id.unique()
    print(
        f"{len(evs_grid_district)} EV loaded "
        f"({len(evs_grid_district.ev_id.unique())} unique) in "
        f"{len(mvgd_bus_ids)} grid districts."
    )

    # Get run metadata
    meta_tech_data = read_simbev_metadata_file(scenario_name, "tech_data")
    meta_run_config = read_simbev_metadata_file(scenario_name, "config").loc[
        "basic"
    ]

    # Generate timeseries for each MVGD
    print("GENERATE MODEL DATA...")
    ctr = 0
    for bus_id in mvgd_bus_ids:
        ctr += 1
        print(
            f"Processing grid district: bus {bus_id}... "
            f"({ctr}/{len(mvgd_bus_ids)})"
        )
        (static_params, load_ts,) = generate_model_data_grid_district(
            scenario_name=scenario_name,
            evs_grid_district=evs_grid_district[
                evs_grid_district.bus_id == bus_id
            ],
            bat_cap_dict=meta_tech_data.battery_capacity.to_dict(),
            run_config=meta_run_config,
        )
        write_model_data_to_db(
            static_params_dict=static_params,
            load_time_series_df=load_ts,
            bus_id=bus_id,
            scenario_name=scenario_name,
            run_config=meta_run_config,
            bat_cap=meta_tech_data.battery_capacity,
        )

def generate_model_data_status2019_remaining():
    """Generates timeseries for status2019 scenario for grid districts which
    has not been processed in the parallel tasks before.
    """
    generate_model_data_bunch(
        scenario_name="status2019",
        bunch=range(MVGD_MIN_COUNT, len(load_grid_district_ids())),
    )

def generate_model_data_eGon2035_remaining():
    """Generates timeseries for eGon2035 scenario for grid districts which
    has not been processed in the parallel tasks before.
    """
    generate_model_data_bunch(
        scenario_name="eGon2035",
        bunch=range(MVGD_MIN_COUNT, len(load_grid_district_ids())),
    )


def generate_model_data_eGon100RE_remaining():
    """Generates timeseries for eGon100RE scenario for grid districts which
    has not been processed in the parallel tasks before.
    """
    generate_model_data_bunch(
        scenario_name="eGon100RE",
        bunch=range(MVGD_MIN_COUNT, len(load_grid_district_ids())),
    )
