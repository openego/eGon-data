import pandas as pd
import geopandas as gpd
import numpy as np
from egon.data import db
from shapely.geometry import Point
from pathlib import Path
import egon.data.config


def weather_id():
    """
    Assign weather data to the weather dependant generators (wind and solar)

    Parameters
    ----------
    *No parameters required
    """

    # Connect to the data base
    con = db.engine()

    cfg = egon.data.config.datasets()["power_plants"]

    # Import table with power plants
    sql = "SELECT * FROM supply.egon_power_plants"
    power_plants = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geom"
    )

    # select the power_plants that are weather dependant
    power_plants = power_plants[
        (power_plants["carrier"] == "solar")
        | (power_plants["carrier"] == "wind_onshore")
        | (power_plants["carrier"] == "wind_offshore")
    ]
    power_plants.set_index("id", inplace=True)

    # Import table with weather data for each technology
    sql = "SELECT * FROM supply.egon_era5_renewable_feedin"
    weather_data = pd.read_sql_query(sql, con)
    weather_data.set_index("w_id", inplace=True)

    # Import weather cells with Id to match with the weather data
    sql = "SELECT * FROM supply.egon_era5_weather_cells"
    weather_cells = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geom"
    )

    # import Germany borders to speed up the matching process
    sql = "SELECT * FROM boundaries.vg250_sta"
    boundaries = gpd.GeoDataFrame.from_postgis(
        sql, con, crs="EPSG:4326", geom_col="geometry"
    )

    # Clip weater data cells using the German boundaries
    weather_cells = gpd.clip(weather_cells, boundaries)

    for weather_id in weather_cells["w_id"]:
        df = gpd.clip(
            power_plants, weather_cells[weather_cells["w_id"] == weather_id]
        )
        power_plant_list = df.index.to_list()
        power_plants.loc[power_plant_list, "weather_cell_id"] = weather_id

    # delete weather dependent power_plants from supply.egon_power_plants
    db.execute_sql(
        f""" 
    DELETE FROM {cfg['target']['schema']}.{cfg['target']['table']} 
    WHERE carrier IN ('wind_onshore', 'solar', 'wind_offshore') 
    """
    )
    
    # assert that the column "bus_id" is set as integer  
    power_plants['bus_id'] = power_plants['bus_id'].apply(
        lambda x: pd.NA if pd.isna(x) else int(x))
    
    # Look for the maximum id in the table egon_power_plants
    sql = (
        "SELECT MAX(id) FROM "
        + cfg["target"]["schema"]
        + "."
        + cfg["target"]["table"]
    )
    max_id = pd.read_sql(sql, con)
    max_id = max_id["max"].iat[0]
    if max_id == None:
        ini_id = 1
    else:
        ini_id = int(max_id + 1)

    # write_table in egon-data database:
    # Reset index
    power_plants.index = pd.RangeIndex(
        start=ini_id, stop=ini_id + len(power_plants), name="id"
    )

    # Insert into database
    power_plants.reset_index().to_postgis(
        cfg["target"]["table"],
        schema=cfg["target"]["schema"],
        con=db.engine(),
        if_exists="append",
    )

    return 0
