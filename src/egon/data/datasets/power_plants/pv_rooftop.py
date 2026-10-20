"""The module containing all code dealing with pv rooftop distribution to MV grid level."""

from loguru import logger
from numpy import isclose
import geopandas as gpd
import pandas as pd

from egon.data import config, db
from egon.data.datasets import load_sources_and_targets
from egon.data.datasets.power_plants.pv_rooftop_buildings import (
    PV_CAP_PER_SQ_M,
    ROOF_FACTOR,
    load_building_data,
)
from egon.data.datasets.scenario_parameters import get_sector_parameters

# assumption on theoretical maximum occupancy of rooftops within an mv grid
# district
# TODO: this is a wild guess
MAX_THEORETICAL_PV_OCCUPANCY = 0.5


def pv_rooftop_per_mv_grid():
    """Execute pv rooftop distribution method per scenario

    Returns
    -------
    None.

    """
    s = config.settings()["egon-data"]["--scenarios"]
    for scn in ["eGon2035", "reGon2037", "reGon2045"]:
         if scn in s:
             pv_rooftop_per_mv_grid_and_scenario(scenario=scn)


def pv_rooftop_per_mv_grid_and_scenario(scenario):
    """Intergate solar rooftop per mv grid district

    The target capacity is distributed to the mv grid districts linear to
    the residential and service electricity demands, based on the target
    capacity per federal state.

    Parameters
    ----------
    scenario : str
        Name of the scenario

    Returns
    -------
    None.

    """
    # Select sources and targets from dataset configuration
    sources, targets = load_sources_and_targets("PowerPlants")

    # Delete existing rows
    db.execute_sql(f"""
        DELETE FROM {targets.tables['generators']}
        WHERE carrier IN ('solar_rooftop')
        AND scn_name = '{scenario}'
        AND bus IN (SELECT bus_id FROM
                    {sources.tables['egon_mv_grid_district']})
        """)

    db.execute_sql(f"""
        DELETE FROM {targets.tables['generator_timeseries']}
        WHERE scn_name = '{scenario}'
        AND generator_id NOT IN (
            SELECT generator_id FROM
            grid.egon_etrago_generator
            WHERE scn_name = '{scenario}')
        """)

    # Select demand per mv grid district
    demand = db.select_dataframe(f"""
         SELECT SUM(demand) as demand,
         b.bus_id, vg250_lan
         FROM {sources.tables['electricity_demand']} a
         JOIN {sources.tables['map_zensus_grid_districts']} b
         ON a.zensus_population_id = b.zensus_population_id
         JOIN {sources.tables['map_grid_boundaries']} c
         ON c.bus_id = b.bus_id
         WHERE scenario = '{scenario}'
         GROUP BY (b.bus_id, vg250_lan)
         """)

    # make sure only grid districts with any buildings are used
    valid_buildings_gdf = load_building_data()

    valid_buildings_gdf = valid_buildings_gdf.assign(
        bus_id=valid_buildings_gdf.bus_id.astype(int),
        overlay_id=valid_buildings_gdf.overlay_id.astype(int),
        max_cap=valid_buildings_gdf.building_area.multiply(
            ROOF_FACTOR * PV_CAP_PER_SQ_M
        ),
    )

    bus_ids = valid_buildings_gdf.bus_id.unique()
    demand = demand.loc[demand.bus_id.isin(bus_ids)]

    # Distribute target capacity per federal state to mv grids
    targets_per_federal_state = db.select_dataframe(
        f"""
        SELECT DISTINCT ON (gen) capacity, gen
        FROM {sources.tables['scenario_capacities']} a
        JOIN {sources.tables['federal_states']} b
        ON a.nuts = b.nuts
        WHERE carrier = 'solar_rooftop'
        AND scenario_name = '{scenario}'
        """,
        index_col="gen",
    )

    demand["share_federal_state"] = demand.groupby(
        "vg250_lan",
        group_keys=False,
    ).demand.apply(lambda grp: grp / grp.sum())

    demand["target_federal_state"] = targets_per_federal_state.capacity[
        demand.vg250_lan
    ].values

    demand.set_index("bus_id", inplace=True)

    capacities = demand["share_federal_state"].mul(
        demand["target_federal_state"]
    )

    # Store data in dataframe
    pv_rooftop = pd.DataFrame(
        data={
            "scn_name": scenario,
            "carrier": "solar_rooftop",
            "bus": demand.index,
            "p_nom": capacities,
            "generator_id": db.next_etrago_id("generator", len(demand)),
        }
    )

    # ensure that no more pv rooftop capacity is allocated to any mv grid
    # district than there is rooftop potential
    max_cap_per_bus_df = (
        valid_buildings_gdf[["max_cap", "bus_id"]].groupby("bus_id").sum()
        * MAX_THEORETICAL_PV_OCCUPANCY
    )

    pv_rooftop = pv_rooftop.merge(
        max_cap_per_bus_df, how="left", left_on="bus", right_index=True
    )

    assert ~pv_rooftop.max_cap.isna().any(), (
        "There are bus IDs within 'pv_rooftop' which are not included within "
        " 'max_cap_per_bus_df'."
    )

    pv_rooftop = pv_rooftop.assign(delta=pv_rooftop.max_cap - pv_rooftop.p_nom)
    loss = pv_rooftop.delta.clip(upper=0).sum()
    total = pv_rooftop.p_nom.sum()

    pv_rooftop = pv_rooftop.assign(
        p_nom=pv_rooftop[["p_nom", "max_cap"]].min(axis=1)
    )

    pos_delta = pv_rooftop.loc[pv_rooftop.delta > 0].delta
    rel_delta = pos_delta / pos_delta.sum()
    add_pv_cap = rel_delta * abs(loss)

    pv_rooftop.loc[add_pv_cap.index, "p_nom"] += add_pv_cap
    pv_rooftop = pv_rooftop.drop(columns=["max_cap", "delta"])

    assert isclose(
        total, pv_rooftop.p_nom.sum()
    ), f"{total} != {pv_rooftop.p_nom.sum()}"

    if loss < 0:
        logger.debug(
            f"{loss:g} MW got redistributed from MV grids with too little "
            f"rooftop potential towards other MV grids."
        )

    # Select feedin timeseries
    weather_cells = db.select_geodataframe(
        f"""
            SELECT w_id, geom
            FROM {sources.tables['weather_cells']}
            """,
        index_col="w_id",
    )

    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT bus_id as bus_id, ST_Centroid(geom) as geom
        FROM {sources.tables['egon_mv_grid_district']}
        """,
        index_col="bus_id",
    )

    # Map centroid of mv grids to weather cells
    join = gpd.sjoin(weather_cells, mv_grid_districts)[["bus_id"]]

    feedin = db.select_dataframe(
        f"""
            SELECT w_id, feedin
            FROM {sources.tables['solar_feedin']}
            WHERE carrier = 'pv'
            AND weather_year = 2011
            """,
        index_col="w_id",
    )

    # Create timeseries only for mv grid districts with pv rooftop
    join = join[join.bus_id.isin(pv_rooftop.bus)]

    timeseries = pd.DataFrame(
        data={
            "scn_name": scenario,
            "temp_id": 1,
            "p_max_pu": feedin.feedin[join.index].values,
            "generator_id": pv_rooftop.generator_id[join.bus_id].values,
        }
    ).set_index("generator_id")

    pv_rooftop = pv_rooftop.set_index("generator_id")
    pv_rooftop["marginal_cost"] = get_sector_parameters(
        "electricity", scenario
    )["marginal_cost"]["solar"]

    # Insert data to database
    pv_rooftop.to_sql(
        targets.get_table_name("generators"),
        schema=targets.get_table_schema("generators"),
        if_exists="append",
        con=db.engine(),
    )

    timeseries.to_sql(
        targets.get_table_name("generator_timeseries"),
        schema=targets.get_table_schema("generator_timeseries"),
        if_exists="append",
        con=db.engine(),
    )
