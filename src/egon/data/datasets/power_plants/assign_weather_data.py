import geopandas as gpd

from egon.data import db


def assign_bus_id(power_plants, cfg):
    """Assigns bus_ids to power plants according to location and voltage level

    Parameters
    ----------
    power_plants : pandas.DataFrame
        Power plants including voltage level

    Returns
    -------
    power_plants : pandas.DataFrame
        Power plants including voltage level and bus_id

    """

    mv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['egon_mv_grid_district']}
        """,
        epsg=4326,
    )

    ehv_grid_districts = db.select_geodataframe(
        f"""
        SELECT * FROM {cfg['sources']['ehv_voronoi']}
        """,
        epsg=4326,
    )

    # Assign power plants in hv and below to hvmv bus
    power_plants_hv = power_plants[power_plants.voltage_level >= 3].index
    if len(power_plants_hv) > 0:
        power_plants.loc[power_plants_hv, "bus_id"] = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_hv)],
            mv_grid_districts,
        ).bus_id

    # Assign power plants in ehv to ehv bus
    power_plants_ehv = power_plants[power_plants.voltage_level < 3].index

    if len(power_plants_ehv) > 0:
        ehv_join = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_ehv)],
            ehv_grid_districts,
        )

        if "bus_id_right" in ehv_join.columns:
            power_plants.loc[power_plants_ehv, "bus_id"] = gpd.sjoin(
                power_plants[power_plants.index.isin(power_plants_ehv)],
                ehv_grid_districts,
            ).bus_id_right

        else:
            power_plants.loc[power_plants_ehv, "bus_id"] = gpd.sjoin(
                power_plants[power_plants.index.isin(power_plants_ehv)],
                ehv_grid_districts,
            ).bus_id

    # Assert that all power plants have a bus_id
    assert power_plants.bus_id.notnull().all(), f"""Some power plants are
    not attached to a bus: {power_plants[power_plants.bus_id.isnull()]}"""

    return power_plants


def add_missing_bus_ids(scn_name):
    """Assign busses by spatal intersection of mvgrid districts or ehv voronois."""

    sql = f"""
                -- Assign missing buses to mv grid district buses for HV and below
                UPDATE supply.egon_power_plants AS epp
                SET bus_id = (
                    SELECT emgd.bus_id
                    FROM grid.egon_mv_grid_district AS emgd
                    WHERE ST_Intersects(epp.geom, emgd.geom)
                    ORDER BY emgd.geom <-> epp.geom
                    LIMIT 1
                )
                WHERE (epp.carrier = 'solar'
                    OR epp.carrier = 'wind_onshore'
                    OR epp.carrier = 'solar_rooftop'
                    OR epp.carrier = 'wind_offshore')
                AND epp.scenario = '{scn_name}'
                AND epp.bus_id is null
                AND epp.voltage_level >= 3; -- HV and below


                -- Assign missing buses to EHV buses for EHV
                UPDATE supply.egon_power_plants AS epp
                SET bus_id2 = (
                    SELECT eesv.bus_id
                    FROM grid.egon_ehv_substation_voronoi AS eesv
                    WHERE ST_Intersects(epp.geom, eesv.geom)
                    ORDER BY eesv.geom <-> epp.geom
                    LIMIT 1
                )
                WHERE (epp.carrier = 'solar'
                    OR epp.carrier = 'wind_onshore'
                    OR epp.carrier = 'solar_rooftop'
                    OR epp.carrier = 'wind_offshore')
                AND epp.scenario = '{scn_name}'
                AND epp.bus_id is null
                AND epp.voltage_level < 3; --EHV


        """

    db.execute_sql(sql)
