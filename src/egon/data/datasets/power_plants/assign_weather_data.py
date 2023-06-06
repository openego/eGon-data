import json

import geopandas as gpd
import pandas as pd
from shapely.geometry import Polygon

from egon.data import db
import egon.data.config


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


def weatherId_and_busId():
    power_plants, cfg, con = find_weather_id()
    power_plants = find_bus_id(power_plants, cfg)
    write_power_plants_table(power_plants, cfg, con)


def find_bus_id(power_plants, cfg):
    # Define bus_id for power plants without it
    power_plants_no_busId = power_plants[power_plants.bus_id.isna()]
    power_plants = power_plants[~power_plants.bus_id.isna()]

    power_plants_no_busId = power_plants_no_busId.drop(columns="bus_id")

    if len(power_plants_no_busId) > 0:
        power_plants_no_busId = assign_bus_id(power_plants_no_busId, cfg)

    power_plants = power_plants.append(power_plants_no_busId)

    return power_plants


def find_weather_id():
    """
    Assign weather data to the weather dependant generators (wind and solar)

    Parameters
    ----------
    *No parameters required
    """

    # Connect to the data base
    con = db.engine()

    cfg = egon.data.config.datasets()["weather_BusID"]

    # Import table with power plants
    sql = f"""
    SELECT * FROM
    {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    """
    power_plants = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geom"
    )

    # select the power_plants that are weather dependant (wind offshore is
    # not included here, because it alredy has weather_id assigned)
    power_plants = power_plants[
        (power_plants["carrier"] == "solar")
        | (power_plants["carrier"] == "wind_onshore")
        | (
            (power_plants["carrier"] == "wind_offshore")
            & (power_plants["weather_cell_id"] == -1)
        )
    ]
    power_plants.set_index("id", inplace=True)

    # Import table with weather data for each technology
    sql = f"""
    SELECT * FROM
    {cfg['sources']['renewable_feedin']['schema']}.
    {cfg['sources']['renewable_feedin']['table']}
    """
    weather_data = pd.read_sql_query(sql, con)
    weather_data.set_index("w_id", inplace=True)

    # Import weather cells with Id to match with the weather data
    sql = f"""
    SELECT * FROM
    {cfg['sources']['weather_cells']['schema']}.
    {cfg['sources']['weather_cells']['table']}
    """
    weather_cells = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geom"
    )

    # import Germany borders to speed up the matching process
    sql = "SELECT * FROM boundaries.vg250_sta"
    sql = f"""
    SELECT * FROM
    {cfg['sources']['boundaries']['schema']}.
    {cfg['sources']['boundaries']['table']}
    """
    boundaries = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geometry"
    )

    baltic_sea = Polygon([(11, 56), (11, 53), (15, 53), (15, 56)])
    north_sea = Polygon([(5, 56), (5, 53), (9, 53), (9, 56)])

    boundaries.loc["baltic_sea", "geometry"] = baltic_sea
    boundaries.loc["north_sea", "geometry"] = north_sea

    # Clip weater data cells using the German boundaries
    weather_cells = gpd.clip(weather_cells, boundaries)

    for weather_id in weather_cells["w_id"]:
        df = gpd.clip(
            power_plants, weather_cells[weather_cells["w_id"] == weather_id]
        )
        power_plant_list = df.index.to_list()
        power_plants.loc[power_plant_list, "weather_cell_id"] = weather_id

    return (power_plants, cfg, con)


def write_power_plants_table(power_plants, cfg, con):
    # delete weather dependent power_plants from supply.egon_power_plants
    db.execute_sql(
        f"""
    DELETE FROM {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    WHERE carrier IN ('wind_onshore', 'solar')
    OR ((carrier = 'wind_offshore') AND (weather_cell_id = '-1'))
    """
    )

    # assert that the column "bus_id" is set as integer
    power_plants["bus_id"] = power_plants["bus_id"].apply(
        lambda x: pd.NA if pd.isna(x) else int(x)
    )

    # assert that the column "weather_cell_id" is set as integer
    power_plants["weather_cell_id"] = power_plants["weather_cell_id"].apply(
        lambda x: pd.NA if pd.isna(x) else int(x)
    )

    # Look for the maximum id in the table egon_power_plants
    sql = f"""
    SELECT MAX(id) FROM
    {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    """
    max_id = pd.read_sql(sql, con)
    max_id = max_id["max"].iat[0]
    if max_id is None:
        ini_id = 1
    else:
        ini_id = int(max_id + 1)

    # write_table in egon-data database:
    # Reset index
    power_plants.index = pd.RangeIndex(
        start=ini_id, stop=ini_id + len(power_plants), name="id"
    )

    # Set json format
    for i in power_plants.index:
        power_plants.sources[i] = json.dumps(power_plants.sources[i])
        power_plants.source_id[i] = json.dumps(power_plants.source_id[i])

    # Insert into database
    power_plants.reset_index().to_postgis(
        name=f"{cfg['sources']['power_plants']['table']}",
        schema=f"{cfg['sources']['power_plants']['schema']}",
        con=con,
        if_exists="append",
    )

    return "Bus_id and Weather_id were updated succesfully"
