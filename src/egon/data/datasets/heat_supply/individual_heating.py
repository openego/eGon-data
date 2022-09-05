"""The central module containing all code dealing with
individual heat supply.

"""
from sqlalchemy import ARRAY, REAL, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import geopandas as gpd
import numpy as np
import pandas as pd
import saio

from egon.data import config, db

engine = db.engine()
Base = declarative_base()


class EgonEtragoTimeSeriesIndividualHeating(Base):
    __tablename__ = "egon_etrago_timeseries_individual_heating"
    __table_args__ = {"schema": "demand"}

    bus_id = Column(Integer, primary_key=True)
    scn_name = Column(String, primary_key=True)
    p_set = Column(ARRAY(REAL))
    q_set = Column(ARRAY(REAL))


def cascade_per_technology(
    heat_per_mv,
    technologies,
    scenario,
    distribution_level,
    max_size_individual_chp=0.05,
):

    """Add plants for individual heat.
    Currently only on mv grid district level.

    Parameters
    ----------
    mv_grid_districts : geopandas.geodataframe.GeoDataFrame
        MV grid districts including the heat demand
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    scenario : str
        Name of the scenario
    max_size_individual_chp : float
        Maximum capacity of an individual chp in MW
    Returns
    -------
    mv_grid_districts : geopandas.geodataframe.GeoDataFrame
        MV grid district which need additional individual heat supply
    technologies : pandas.DataFrame
        List of supply technologies and their parameters
    append_df : pandas.DataFrame
        List of plants per mv grid for the selected technology

    """
    sources = config.datasets()["heat_supply"]["sources"]

    tech = technologies[technologies.priority == technologies.priority.max()]

    # Distribute heat pumps linear to remaining demand.
    if tech.index == "heat_pump":

        if distribution_level == "federal_state":
            # Select target values per federal state
            target = db.select_dataframe(
                f"""
                    SELECT DISTINCT ON (gen) gen as state, capacity
                    FROM {sources['scenario_capacities']['schema']}.
                    {sources['scenario_capacities']['table']} a
                    JOIN {sources['federal_states']['schema']}.
                    {sources['federal_states']['table']} b
                    ON a.nuts = b.nuts
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'residential_rural_heat_pump'
                    """,
                index_col="state",
            )

            heat_per_mv["share"] = heat_per_mv.groupby(
                "state"
            ).remaining_demand.apply(lambda grp: grp / grp.sum())

            append_df = (
                heat_per_mv["share"]
                .mul(target.capacity[heat_per_mv["state"]].values)
                .reset_index()
            )
        else:
            # Select target value for Germany
            target = db.select_dataframe(
                f"""
                    SELECT SUM(capacity) AS capacity
                    FROM {sources['scenario_capacities']['schema']}.
                    {sources['scenario_capacities']['table']} a
                    WHERE scenario_name = '{scenario}'
                    AND carrier = 'residential_rural_heat_pump'
                    """
            )

            heat_per_mv["share"] = (
                heat_per_mv.remaining_demand
                / heat_per_mv.remaining_demand.sum()
            )

            append_df = (
                heat_per_mv["share"].mul(target.capacity[0]).reset_index()
            )

        append_df.rename(
            {"bus_id": "mv_grid_id", "share": "capacity"}, axis=1, inplace=True
        )

    elif tech.index == "gas_boiler":

        append_df = pd.DataFrame(
            data={
                "capacity": heat_per_mv.remaining_demand.div(
                    tech.estimated_flh.values[0]
                ),
                "carrier": "residential_rural_gas_boiler",
                "mv_grid_id": heat_per_mv.index,
                "scenario": scenario,
            }
        )

    if append_df.size > 0:
        append_df["carrier"] = tech.index[0]
        heat_per_mv.loc[
            append_df.mv_grid_id, "remaining_demand"
        ] -= append_df.set_index("mv_grid_id").capacity.mul(
            tech.estimated_flh.values[0]
        )

    heat_per_mv = heat_per_mv[heat_per_mv.remaining_demand >= 0]

    technologies = technologies.drop(tech.index)

    return heat_per_mv, technologies, append_df


def cascade_heat_supply_indiv(scenario, distribution_level, plotting=True):
    """Assigns supply strategy for individual heating in four steps.

    1.) all small scale CHP are connected.
    2.) If the supply can not  meet the heat demand, solar thermal collectors
        are attached. This is not implemented yet, since individual
        solar thermal plants are not considered in eGon2035 scenario.
    3.) If this is not suitable, the mv grid is also supplied by heat pumps.
    4.) The last option are individual gas boilers.

    Parameters
    ----------
    scenario : str
        Name of scenario
    plotting : bool, optional
        Choose if individual heating supply is plotted. The default is True.

    Returns
    -------
    resulting_capacities : pandas.DataFrame
        List of plants per mv grid

    """

    sources = config.datasets()["heat_supply"]["sources"]

    # Select residential heat demand per mv grid district and federal state
    heat_per_mv = db.select_geodataframe(
        f"""
        SELECT d.bus_id as bus_id, SUM(demand) as demand,
        c.vg250_lan as state, d.geom
        FROM {sources['heat_demand']['schema']}.
        {sources['heat_demand']['table']} a
        JOIN {sources['map_zensus_grid']['schema']}.
        {sources['map_zensus_grid']['table']} b
        ON a.zensus_population_id = b.zensus_population_id
        JOIN {sources['map_vg250_grid']['schema']}.
        {sources['map_vg250_grid']['table']} c
        ON b.bus_id = c.bus_id
        JOIN {sources['mv_grids']['schema']}.
        {sources['mv_grids']['table']} d
        ON d.bus_id = c.bus_id
        WHERE scenario = '{scenario}'
        AND a.zensus_population_id NOT IN (
            SELECT zensus_population_id
            FROM {sources['map_dh']['schema']}.{sources['map_dh']['table']}
            WHERE scenario = '{scenario}')
        GROUP BY d.bus_id, vg250_lan, geom
        """,
        index_col="bus_id",
    )

    # Store geometry of mv grid
    geom_mv = heat_per_mv.geom.centroid.copy()

    # Initalize Dataframe for results
    resulting_capacities = pd.DataFrame(
        columns=["mv_grid_id", "carrier", "capacity"]
    )

    # Set technology data according to
    # http://www.wbzu.de/seminare/infopool/infopool-bhkw
    # TODO: Add gas boilers and solar themal (eGon100RE)
    technologies = pd.DataFrame(
        index=["heat_pump", "gas_boiler"],
        columns=["estimated_flh", "priority"],
        data={"estimated_flh": [4000, 8000], "priority": [2, 1]},
    )

    # In the beginning, the remaining demand equals demand
    heat_per_mv["remaining_demand"] = heat_per_mv["demand"]

    # Connect new technologies, if there is still heat demand left
    while (len(technologies) > 0) and (len(heat_per_mv) > 0):
        # Attach new supply technology
        heat_per_mv, technologies, append_df = cascade_per_technology(
            heat_per_mv, technologies, scenario, distribution_level
        )
        # Collect resulting capacities
        resulting_capacities = resulting_capacities.append(
            append_df, ignore_index=True
        )

    if plotting:
        plot_heat_supply(resulting_capacities)

    return gpd.GeoDataFrame(
        resulting_capacities,
        geometry=geom_mv[resulting_capacities.mv_grid_id].values,
    )


def plot_heat_supply(resulting_capacities):

    from matplotlib import pyplot as plt

    mv_grids = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_mv_grid_district
        """,
        index_col="bus_id",
    )

    for c in ["CHP", "heat_pump"]:
        mv_grids[c] = (
            resulting_capacities[resulting_capacities.carrier == c]
            .set_index("mv_grid_id")
            .capacity
        )

        fig, ax = plt.subplots(1, 1)
        mv_grids.boundary.plot(linewidth=0.2, ax=ax, color="black")
        mv_grids.plot(
            ax=ax,
            column=c,
            cmap="magma_r",
            legend=True,
            legend_kwds={
                "label": f"Installed {c} in MW",
                "orientation": "vertical",
            },
        )
        plt.savefig(f"plots/individual_heat_supply_{c}.png", dpi=300)


def get_buildings_with_decentral_heat_demand_in_mv_grid(scenario, mv_grid_id):
    """
    Returns building IDs of buildings with decentral heat demand in given MV grid.

    As cells with district heating differ between scenarios, this is also depending
    on the scenario.

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    pd.Index(int)
        Building IDs (as int) of buildings with decentral heat demand in given MV grid.
        Type is pandas Index to avoid errors later on when it is used in a query.

    """

    # get zensus cells in grid
    zensus_population_ids = db.select_dataframe(
        f"""
        SELECT zensus_population_id
        FROM boundaries.egon_map_zensus_grid_districts
        WHERE bus_id = {mv_grid_id}
        """,
        index_col=None,
    ).zensus_population_id.values

    # convert to pd.Index (otherwise type is np.int64, which will for some reason
    # throw an error when used in a query)
    zensus_population_ids = pd.Index(zensus_population_ids)

    # get zensus cells with district heating
    from egon.data.datasets.district_heating_areas import (
        MapZensusDistrictHeatingAreas,
    )

    with db.session_scope() as session:
        query = session.query(
            MapZensusDistrictHeatingAreas.zensus_population_id,
        ).filter(
            MapZensusDistrictHeatingAreas.scenario == scenario,
            MapZensusDistrictHeatingAreas.zensus_population_id.in_(
                zensus_population_ids
            ),
        )

    cells_with_dh = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).zensus_population_id.values

    # remove zensus cells with district heating
    zensus_population_ids = zensus_population_ids.drop(
        cells_with_dh, errors="ignore"
    )

    # get buildings with decentral heat demand
    engine = db.engine()
    saio.register_schema("demand", engine)
    from saio.demand import heat_timeseries_selected_profiles

    with db.session_scope() as session:
        query = session.query(
            heat_timeseries_selected_profiles.building_id,
        ).filter(
            heat_timeseries_selected_profiles.zensus_population_id.in_(
                zensus_population_ids
            )
        )

    buildings_with_heat_demand = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).building_id.values

    # convert to pd.Index (otherwise type is np.int64, which will for some reason
    # throw an error when used in a query)
    return pd.Index(buildings_with_heat_demand)


def get_total_heat_pump_capacity_of_mv_grid(scenario, mv_grid_id):
    """
    Returns total heat pump capacity per grid that was previously defined (by NEP or
    pypsa-eur-sec).

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    mv_grid_id : int
        ID of MV grid.

    Returns
    --------
    float
        Total heat pump capacity in MW in given MV grid.

    """
    from egon.data.datasets.heat_supply import EgonIndividualHeatingSupply

    with db.session_scope() as session:
        query = (
            session.query(
                EgonIndividualHeatingSupply.mv_grid_id,
                EgonIndividualHeatingSupply.capacity,
            )
            .filter(EgonIndividualHeatingSupply.scenario == scenario)
            .filter(EgonIndividualHeatingSupply.carrier == "heat_pump")
            .filter(EgonIndividualHeatingSupply.mv_grid_id == mv_grid_id)
        )

    hp_cap_mv_grid = pd.read_sql(
        query.statement, query.session.bind, index_col="mv_grid_id"
    ).capacity.values[0]

    return hp_cap_mv_grid


def get_heat_demand_timeseries_per_building(scenario, building_ids):
    """
    Gets heat demand time series for all given buildings.

    ToDo: CTS demand still missing!! Also maybe use other function to make it faster.

    Parameters
    -----------
    scenario : str
        Name of scenario. Can be either "eGon2035" or "eGon100RE".
    building_ids : pd.Index(int)
        Building IDs (as int) of buildings to get heat demand time series for.

    Returns
    --------
    pd.DataFrame
        Dataframe with hourly heat demand in MW for entire year. Index of the dataframe
        contains the time steps and columns the building ID.

    """
    from egon.data.datasets.heat_demand_timeseries import (
        create_timeseries_for_building,
    )

    heat_demand_ts = pd.DataFrame()
    for building_id in building_ids:
        tmp = create_timeseries_for_building(building_id, scenario)
        heat_demand_ts = pd.concat([heat_demand_ts, tmp], axis=1)
    return heat_demand_ts


def determine_minimum_hp_capacity_per_building(
    peak_heat_demand, flexibility_factor=24 / 18, cop=1.7
):
    """
    Determines minimum required heat pump capacity

    Parameters
    ----------
    peak_heat_demand : pd.Series
        Series with peak heat demand per building in MW. Index contains the building ID.
    flexibility_factor : float
        Factor to overdimension the heat pump to allow for some flexible dispatch in
        times of high heat demand. Per default, a factor of 24/18 is used, to take into
        account

    Returns
    -------
    pd.Series
        Pandas series with minimum required heat pump capacity per building in MW.

    """
    return peak_heat_demand * flexibility_factor / cop


def determine_buildings_with_hp_in_mv_grid(
    hp_cap_mv_grid, min_hp_cap_per_building
):
    """
    Distributes given total heat pump capacity to buildings based on their peak
    heat demand.

    Parameters
    -----------
    hp_cap_mv_grid : float
        Total heat pump capacity in MW in given MV grid.
    min_hp_cap_per_building : pd.Series
        Pandas series with minimum required heat pump capacity per building in MW.

    Returns
    -------
    pd.Index(int)
        Building IDs (as int) of buildings to get heat demand time series for.

    """
    building_ids = min_hp_cap_per_building.index

    # get buildings with PV to give them a higher priority when selecting buildings
    # a heat pump will be allocated to
    engine = db.engine()
    saio.register_schema("supply", engine)
    from saio.supply import egon_power_plants_pv_roof_building

    with db.session_scope() as session:
        query = session.query(
            egon_power_plants_pv_roof_building.building_id
        ).filter(
            egon_power_plants_pv_roof_building.building_id.in_(building_ids)
        )

    buildings_with_pv = pd.read_sql(
        query.statement, query.session.bind, index_col=None
    ).building_id.values

    # set different weights for buildings with PV and without PV
    weight_with_pv = 1.5
    weight_without_pv = 1.0
    weights = pd.concat(
        [
            pd.DataFrame(
                {"weight": weight_without_pv},
                index=building_ids.drop(buildings_with_pv, errors="ignore"),
            ),
            pd.DataFrame({"weight": weight_with_pv}, index=buildings_with_pv),
        ]
    )
    # normalise weights (probability needs to add up to 1)
    weights.weight = weights.weight / weights.weight.sum()

    # get random order at which buildings are chosen
    np.random.seed(db.credentials()["--random-seed"])
    buildings_with_hp_order = np.random.choice(
        weights.index,
        size=len(weights),
        replace=False,
        p=weights.weight.values,
    )

    # select buildings until HP capacity in MV grid is reached (some rest capacity
    # will remain)
    hp_cumsum = min_hp_cap_per_building.loc[buildings_with_hp_order].cumsum()
    buildings_with_hp = hp_cumsum[hp_cumsum <= hp_cap_mv_grid].index

    return buildings_with_hp


def desaggregate_hp_capacity(min_hp_cap_per_building, hp_cap_mv_grid):
    """
    Desaggregates the required total heat pump capacity to buildings.

    All buildings are previously assigned a minimum required heat pump capacity. If
    the total heat pump capacity exceeds this, larger heat pumps are assigned.

    Parameters
    ------------
    min_hp_cap_per_building : pd.Series
        Pandas series with minimum required heat pump capacity per building in MW.
    hp_cap_mv_grid : float
        Total heat pump capacity in MW in given MV grid.

    Returns
    --------
    pd.Series
        Pandas series with heat pump capacity per building in MW.

    """
    # distribute remaining capacity to all buildings with HP depending on installed
    # HP capacity

    allocated_cap = min_hp_cap_per_building.sum()
    remaining_cap = hp_cap_mv_grid - allocated_cap

    if remaining_cap < 0:
        # ToDo raise warning?
        # TODO alternative outsource sanity check
        #  only possible if peak_load is determined beforehand
        return

    fac = remaining_cap / allocated_cap
    hp_cap_per_building = (
        min_hp_cap_per_building * fac + min_hp_cap_per_building
    )
    return hp_cap_per_building


def determine_hp_capacity_per_building(scenario):
    """
    Parameters
    -----------
    scenario : str
        "pypsa-eur-sec", "eGon2035", "eGon100RE"

    """

    # get all MV grid IDs
    mv_grid_ids = db.select_dataframe(
        f"""
        SELECT bus_id
        FROM grid.egon_mv_grid_district
        """,
        index_col=None,
    ).bus_id.values

    for mv_grid_id in mv_grid_ids:

        # determine minimum required heat pump capacity per building
        building_ids = get_buildings_with_decentral_heat_demand_in_mv_grid(
            scenario, mv_grid_id
        )
        # TODO alternative get peak demand from db?
        heat_demand_ts = get_heat_demand_timeseries_per_building(
            scenario, building_ids
        )
        # ToDo Write peak heat demand to table?
        min_hp_cap_buildings = determine_minimum_hp_capacity_per_building(
            heat_demand_ts.max()
        )

        # in case this function is called to create pypsa-eur-sec input, only the
        # minimum required heat pump capacity per MV grid is needed
        if scenario == "pypsa-eur-sec":
            min_hp_cap_buildings.sum()
            # ToDo Write minimum required capacity to table for pypsa-eur-sec input
            return

        # in case this function is called to create data for 2035 scenario, the
        # buildings with heat pumps are determined; for 2050 scenario all buildings
        # with decentral heating system get a heat pump
        hp_cap_grid = get_total_heat_pump_capacity_of_mv_grid(
            scenario, mv_grid_id
        )
        if scenario == "eGon2035":
            buildings_with_hp = determine_buildings_with_hp_in_mv_grid(
                hp_cap_grid, min_hp_cap_buildings
            )
            min_hp_cap_buildings = min_hp_cap_buildings.loc[buildings_with_hp]

        # distribute total heat pump capacity to all buildings with HP
        hp_cap_per_building = desaggregate_hp_capacity(
            min_hp_cap_buildings, hp_cap_grid
        )

        # ToDo Write desaggregated HP capacity to table
        heat_timeseries_hp_buildings_mv_grid = heat_demand_ts.loc[
            :, hp_cap_per_building.index
        ].sum()

        # ToDo Write aggregated heat demand time series of buildings with HP to
        #  table to be used in eTraGo - egon_etrago_timeseries_individual_heating
        # EgonEtragoTimeSeriesIndividualHeating

        # # Change format
        #     data = CTS_grid.drop(columns="scenario")
        #     df_etrago_cts_heat_profiles = pd.DataFrame(
        #         index=data.index, columns=["scn_name", "p_set"]
        #     )
        #     df_etrago_cts_heat_profiles.p_set = data.values.tolist()
        #     df_etrago_cts_heat_profiles.scn_name = CTS_grid["scenario"]
        #     df_etrago_cts_heat_profiles.reset_index(inplace=True)
        #
        #     # Drop and recreate Table if exists
        #     EgonEtragoTimeSeriesIndividualHeating.__table__.drop(bind=db.engine(), checkfirst=True)
        #     EgonEtragoTimeSeriesIndividualHeating.__table__.create(bind=db.engine(), checkfirst=True)
        #
        #     # Write heat ts into db
        #     with db.session_scope() as session:
        #         session.bulk_insert_mappings(
        #             EgonEtragoTimeSeriesIndividualHeating,
        #             df_etrago_cts_heat_profiles.to_dict(orient="records"),
        #         )
        # ToDo Write other heat demand time series to database - gas voronoi
        #  (grid - egon_gas_voronoi mit carrier CH4)
        #  erstmal intermediate table


def determine_hp_cap_pypsa_eur_sec():
    """Wrapper function to determine heat pump capacities for scenario
    pypsa-eur-sec. Only the minimum required heat pump capacity per MV grid is
    exported to db
    """
    determine_hp_capacity_per_building(scenario="pypsa-eur-sec")


def determine_hp_cap_eGon2035():
    """Wrapper function to determine Heat Pump capacities
    for scenario eGon2035. Only selected buildings get a heat pump capacity
    assigned. Buildings with PV rooftop are more likely to be assigned.
    """
    determine_hp_capacity_per_building(scenario="eGon2035")


def determine_hp_cap_eGon100RE():
    """Wrapper function to determine Heat Pump capacities
    for scenario eGon100RE. All buildings without district heating get a heat
    pump capacity assigned.
    """
    determine_hp_capacity_per_building(scenario="eGon100RE")
