"""Module for repeated bus insertion tasks
"""
import geopandas as gpd
from egon.data import db
from geoalchemy2 import Geometry


def initialise_bus_insertion(carrier, target, scenario="eGon2035"):
    """ Initialise bus insertion to etrago table

    Parameters
    ----------
    carrier : str
        Name of the carrier.
    target : dict
        Target schema and table information.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    gdf : geopandas.GeoDataFrame
        Empty GeoDataFrame to store buses to.

    """
    # Delete existing buses
    db.execute_sql(
        f"""
        DELETE FROM {target['schema']}.{target['table']}
        WHERE scn_name = '{scenario}'
        AND carrier = '{carrier}'
        """
    )

    # initalize dataframe for new buses
    return (
        gpd.GeoDataFrame(
            columns=["scn_name", "bus_id", "carrier", "x", "y", "geom"]
        )
        .set_geometry("geom")
        .set_crs(epsg=4326)
    )


def finalize_bus_insertion(bus_data, carrier, target, scenario="eGon2035"):
    """ Finalize bus insertion to etrago table

    Parameters
    ----------
    bus_data : geopandas.GeoDataFrame
        GeoDataFrame containing the processed bus data.
    carrier : str
        Name of the carrier.
    target : dict
        Target schema and table information.
    scenario : str, optional
        Name of the scenario The default is 'eGon2035'.

    Returns
    -------
    bus_data : geopandas.GeoDataFrame
        GeoDataFrame containing the inserted bus data.
    """
    # Select unused index of buses
    next_bus_id = db.next_etrago_id("bus")

    # Insert values into dataframe
    bus_data["scn_name"] = scenario
    bus_data["carrier"] = carrier
    bus_data["x"] = bus_data.geom.x
    bus_data["y"] = bus_data.geom.y
    bus_data["bus_id"] = range(next_bus_id, next_bus_id + len(bus_data))

    # Insert data into database
    bus_data.to_postgis(
        target["table"],
        schema=target["schema"],
        if_exists="append",
        con=db.engine(),
        dtype={"geom": Geometry()},
    )

    return bus_data
