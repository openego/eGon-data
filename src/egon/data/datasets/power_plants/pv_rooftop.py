"""The module containing all code dealing with pv rooftop distribution.
"""
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets.scenario_parameters import get_sector_parameters


def pv_rooftop_per_mv_grid():
    """Execute pv rooftop distribution method per scenario

    Returns
    -------
    None.

    """

    pv_rooftop_per_mv_grid_and_scenario(
        scenario="eGon2035", level="federal_state"
    )

    pv_rooftop_per_mv_grid_and_scenario(scenario="eGon100RE", level="national")


def pv_rooftop_per_mv_grid_and_scenario(scenario, level):
    """Intergate solar rooftop per mv grid district

    The target capacity is distributed to the mv grid districts linear to
    the residential and service electricity demands.

    Parameters
    ----------
    scenario : str, optional
        Name of the scenario
    level : str, optional
        Choose level of target values.

    Returns
    -------
    None.

    """
    # Select sources and targets from dataset configuration
    sources = config.datasets()["solar_rooftop"]["sources"]
    targets = config.datasets()["solar_rooftop"]["targets"]

    # Delete existing rows
    db.execute_sql(
        f"""
        DELETE FROM {targets['generators']['schema']}.
        {targets['generators']['table']}
        WHERE carrier IN ('solar_rooftop')
        AND scn_name = '{scenario}'
        AND bus IN (SELECT bus_id FROM
                    {sources['egon_mv_grid_district']['schema']}.
                    {sources['egon_mv_grid_district']['table']}            )
        """
    )

    db.execute_sql(
        f"""
        DELETE FROM {targets['generator_timeseries']['schema']}.
        {targets['generator_timeseries']['table']}
        WHERE scn_name = '{scenario}'
        AND generator_id NOT IN (
            SELECT generator_id FROM
            grid.egon_etrago_generator
            WHERE scn_name = '{scenario}')
        """
    )

    # Select demand per mv grid district
    demand = db.select_dataframe(
        f"""
         SELECT SUM(demand) as demand,
         b.bus_id, vg250_lan
         FROM {sources['electricity_demand']['schema']}.
         {sources['electricity_demand']['table']} a
         JOIN {sources['map_zensus_grid_districts']['schema']}.
         {sources['map_zensus_grid_districts']['table']} b
         ON a.zensus_population_id = b.zensus_population_id
         JOIN {sources['map_grid_boundaries']['schema']}.
         {sources['map_grid_boundaries']['table']} c
         ON c.bus_id = b.bus_id
         WHERE scenario = '{scenario}'
         GROUP BY (b.bus_id, vg250_lan)
         """
    )

    # Distribute to mv grids per federal state or Germany
    if level == "federal_state":
        targets_per_federal_state = db.select_dataframe(
            f"""
            SELECT DISTINCT ON (gen) capacity, gen
            FROM {sources['scenario_capacities']['schema']}.
            {sources['scenario_capacities']['table']} a
            JOIN {sources['federal_states']['schema']}.
            {sources['federal_states']['table']} b
            ON a.nuts = b.nuts
            WHERE carrier = 'solar_rooftop'
            AND scenario_name = '{scenario}'
            """,
            index_col="gen",
        )

        demand["share_federal_state"] = demand.groupby(
            "vg250_lan"
        ).demand.apply(lambda grp: grp / grp.sum())

        demand["target_federal_state"] = targets_per_federal_state.capacity[
            demand.vg250_lan
        ].values

        demand.set_index("bus_id", inplace=True)

        capacities = demand["share_federal_state"].mul(
            demand["target_federal_state"]
        )
    else:

        target = db.select_dataframe(
            f"""
            SELECT capacity
            FROM {sources['scenario_capacities']['schema']}.
            {sources['scenario_capacities']['table']} a
            WHERE carrier = 'solar_rooftop'
            AND scenario_name = '{scenario}'
            """
        ).capacity[0]

        demand["share_country"] = demand.demand / demand.demand.sum()

        demand.set_index("bus_id", inplace=True)

        capacities = demand["share_country"].mul(target)

    # Select next id value
    new_id = db.next_etrago_id("generator")

    # Store data in dataframe
    pv_rooftop = pd.DataFrame(
        data={
            "scn_name": scenario,
            "carrier": "solar_rooftop",
            "bus": demand.index,
            "p_nom": capacities,
            "generator_id": range(new_id, new_id + len(demand)),
        }
    )

    # Select feedin timeseries
    weather_cells = db.select_geodataframe(
        f"""
            SELECT w_id, geom
            FROM {sources['weather_cells']['schema']}.
                {sources['weather_cells']['table']}
            """,
        index_col="w_id",
    )

    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT bus_id as bus_id, ST_Centroid(geom) as geom
        FROM {sources['egon_mv_grid_district']['schema']}.
        {sources['egon_mv_grid_district']['table']}
        """,
        index_col="bus_id",
    )

    # Map centroid of mv grids to weather cells
    join = gpd.sjoin(weather_cells, mv_grid_districts)[["index_right"]]

    feedin = db.select_dataframe(
        f"""
            SELECT w_id, feedin
            FROM {sources['solar_feedin']['schema']}.
                {sources['solar_feedin']['table']}
            WHERE carrier = 'pv'
            AND weather_year = 2011
            """,
        index_col="w_id",
    )

    # Create timeseries only for mv grid districts with pv rooftop
    join = join[join.index_right.isin(pv_rooftop.bus)]

    timeseries = pd.DataFrame(
        data={
            "scn_name": scenario,
            "temp_id": 1,
            "p_max_pu": feedin.feedin[join.index].values,
            "generator_id": pv_rooftop.generator_id[join.index_right].values,
        }
    ).set_index("generator_id")

    pv_rooftop = pv_rooftop.set_index("generator_id")
    pv_rooftop["marginal_cost"] = get_sector_parameters(
        "electricity", scenario
    )["marginal_cost"]["solar"]

    # Insert data to database
    pv_rooftop.to_sql(
        targets["generators"]["table"],
        schema=targets["generators"]["schema"],
        if_exists="append",
        con=db.engine(),
    )

    timeseries.to_sql(
        targets["generator_timeseries"]["table"],
        schema=targets["generator_timeseries"]["schema"],
        if_exists="append",
        con=db.engine(),
    )
