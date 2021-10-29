import geopandas as gpd
from egon.data import db
import egon.data.config
from egon.data.datasets import Dataset
import numpy as np
import pandas as pd
from pathlib import Path
import psycopg2
import rioxarray
from shapely.geometry import Point
import xarray as xr
"""
class egon_etrago_gen(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="dlr",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(
                group_gen,               
            ),
        )
"""

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
        """
        SELECT * FROM grid.egon_mv_grid_district
        """,
        epsg=4326,
    )

    ehv_grid_districts = db.select_geodataframe(
        """
        SELECT * FROM grid.egon_ehv_substation_voronoi
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
        power_plants.loc[power_plants_ehv, "bus_id"] = gpd.sjoin(
            power_plants[power_plants.index.isin(power_plants_ehv)],
            ehv_grid_districts,
        ).bus_id_right

    # Assert that all power plants have a bus_id
    assert power_plants.bus_id.notnull().all(), f"""Some power plants are
    not attached to a bus: {power_plants[power_plants.bus_id.isnull()]}"""

    return power_plants



def load_tables(con, cfg): 
    sql = f"""
    SELECT * FROM
    {cfg['sources']['power_plants']['schema']}.
    {cfg['sources']['power_plants']['table']}
    """
    power_plants = gpd.GeoDataFrame.from_postgis(sql, con, crs="EPSG:4326")
     
    sql = f"""
    SELECT * FROM
    {cfg['sources']['renewable_feedin']['schema']}.
    {cfg['sources']['renewable_feedin']['table']}
    """
    renew_feedin = pd.read_sql(sql, con)
     
    sql = f"""
    SELECT * FROM
    {cfg['sources']['weather_cells']['schema']}.
    {cfg['sources']['weather_cells']['table']}
    """
    weather_cells = gpd.GeoDataFrame.from_postgis(sql, con, crs="EPSG:4326")
     
    sql = f"""
    SELECT * FROM
    {cfg['targets']['etrago_generators']['schema']}.
    {cfg['targets']['etrago_generators']['table']}
    """
    etrago_gen_orig = pd.read_sql(sql, con)
    
    return power_plants, renew_feedin, weather_cells, etrago_gen_orig

#def group_gen():
# Connect to the data base
con = db.engine()
cfg = egon.data.config.datasets()["generators_etrago"]

# Load required tables
power_plants, renew_feedin, weather_cells, etrago_gen_orig = load_tables(
    con, cfg
)
##################ERASE AFTER SOLAR AND WIND HAVE BUS_ID#######################
bus_id = power_plants.bus_id.dropna().values
power_plants['no_bus'] = power_plants.bus_id.isna()

def define_bus_id(serie):
    if serie['no_bus'] == True:
        number = np.random.randint(0, 100)
        return bus_id[number]
    else:
        return serie['bus_id']

power_plants['bus_id'] = power_plants.apply(define_bus_id, axis= 1)
##################ERASE AFTER SOLAR AND WIND HAVE BUS_ID#######################

dic = {}
for bus, df in power_plants.groupby(by='bus_id'):
    dic[bus] = df
































