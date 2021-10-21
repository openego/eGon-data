"""The central module containing all code dealing with heat sector in etrago
"""
import pandas as pd
from egon.data.datasets.hydrogen_etrago.storage import insert_H2_overground_storage
import geopandas as gpd
from egon.data import db, config
from egon.data.datasets.insert_etrago_buses import (
    initialise_bus_insertion,
    finalize_bus_insertion,
)
from egon.data.datasets import Dataset


def insert_hydrogen_buses():
    """ Insert hydrogen buses to etrago table

    Hydrogen buses are divided into cavern and methane grid attached buses

    Parameters
    ----------
    carrier : str
        Name of the carrier, either 'hydrogen_cavern' or 'hydrogen_grid'
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    """
    carrier = "H2"
    scenario = "eGon2035"
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    target = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_buses"]
    # initalize dataframe for hydrogen buses
    hydrogen_buses = initialise_bus_insertion(carrier, target)

    # work on individual DataFrames
    hydrogen_cavern = hydrogen_buses.copy()
    hydrogen_grid = hydrogen_buses.copy()

    insert_H2_buses_from_saltcavern(hydrogen_cavern, carrier, sources, target)
    insert_H2_buses_from_CH4_grid(hydrogen_grid, carrier, target)


def insert_H2_buses_from_saltcavern(gdf, carrier, sources, target):
    """Insert the H2 buses based saltcavern locations to db.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    carrier : str
        Name of the carrier.
    sources : dict
        Sources schema and table information.
    target : dict
        Target schema and table information.

    """

    # buses for saltcaverns
    locations = db.select_geodataframe(
        f"""
        SELECT id, geometry as geom
        FROM  {sources['saltcavern_data']['schema']}.
        {sources['saltcavern_data']['table']}""",
        index_col="id",
    )
    gdf.geom = locations.centroid.to_crs(epsg=4326)
    finalize_bus_insertion(gdf, carrier, target)


def insert_H2_buses_from_CH4_grid(gdf, carrier, target):
    """Insert the H2 buses based on CH4 grid to db.

    Parameters
    ----------
    gdf : geopandas.GeoDataFrame
        GeoDataFrame containing the empty bus data.
    carrier : str
        Name of the carrier.
    target : dict
        Target schema and table information.

    """
    # Connect to local database
    engine = db.engine()

    # Select the CH4 buses
    sql_CH4 = """SELECT bus_id, scn_name, geom
                FROM grid.egon_etrago_bus
                WHERE carrier = 'CH4';"""

    gdf_H2 = db.select_geodataframe(sql_CH4, epsg=4326)
    finalize_bus_insertion(gdf_H2, carrier, target)


class HydrogenBusEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenBusEtrago",
            version="0.0.0dev",
            dependencies=dependencies,
            tasks=(insert_hydrogen_buses),
        )


class HydrogenStoreEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenStoreEtrago",
            version="0.0.0",
            dependencies=dependencies,
            tasks=(insert_H2_overground_storage),
        )
