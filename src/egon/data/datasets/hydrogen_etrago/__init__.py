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
    scenario = "eGon2035"
    sources = config.datasets()["etrago_hydrogen"]["sources"]
    target = config.datasets()["etrago_hydrogen"]["targets"]["hydrogen_buses"]
    # initalize dataframe for hydrogen buses
    carrier = "H2_saltcavern"
    hydrogen_buses = initialise_bus_insertion(carrier, target)
    insert_H2_buses_from_saltcavern(hydrogen_buses, carrier, sources, target)

    carrier = "H2_grid"
    hydrogen_buses = initialise_bus_insertion(carrier, target)
    insert_H2_buses_from_CH4_grid(hydrogen_buses, carrier, target)


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
    # CH4 bus ids and respective hydrogen bus ids are writte to db for
    # later use (CH4 grid to H2 links)
    buses_CH4 = gdf_H2[['bus_id', 'scn_name']].copy()

    gdf_H2 = finalize_bus_insertion(gdf_H2, carrier, target)

    gdf_H2_CH4 = gdf_H2[['bus_id']].rename(columns={'bus_id': 'bus_H2'})
    gdf_H2_CH4['bus_CH4'] = buses_CH4['bus_id']
    gdf_H2_CH4['scn_name'] = buses_CH4['scn_name']

    # Insert data to db
    gdf_H2_CH4.to_sql(
        "egon_etrago_ch4_h2",
        engine,
        schema="grid",
        index=False,
        if_exists="replace",
    )


class HydrogenBusEtrago(Dataset):
    def __init__(self, dependencies):
        super().__init__(
            name="HydrogenBusEtrago",
            version="0.0.0",
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
