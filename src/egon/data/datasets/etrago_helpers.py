"""Module for repeated bus insertion tasks
"""
from geoalchemy2 import Geometry
import geopandas as gpd

from egon.data import db


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


def copy_and_modify_links(from_scn, to_scn, filter_dict):

    where_clause = ""
    for column, filters in filter_dict.items():
        where_clause += (
            column
            + " IN "
            + str(tuple(filters)).replace("',)", "')")
            + " AND "
        )

    gdf = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_link
        WHERE {where_clause} scn_name = '{from_scn}' AND
        bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{from_scn}' AND country = 'DE'
        ) AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{from_scn}' AND country = 'DE'
        );
        """,
        epsg=4326
    )

    gdf.loc[gdf["scn_name"] == from_scn, "scn_name"] = to_scn

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_link
        WHERE {where_clause} scn_name = '{to_scn}' AND
        bus0 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{to_scn}' AND country = 'DE'
        ) AND bus1 IN (
            SELECT bus_id FROM grid.egon_etrago_bus
            WHERE scn_name = '{to_scn}' AND country = 'DE'
        );
        """
    )

    gdf.to_postgis(
        "egon_etrago_link",
        schema="grid",
        if_exists="append",
        con=db.engine(),
        dtype={"geom": Geometry(), "topo": Geometry()},
    )


def copy_and_modify_buses(from_scn, to_scn, filter_dict):

    where_clause = ""
    for column, filters in filter_dict.items():
        where_clause += (
            column
            + " IN "
            + str(tuple(filters)).replace("',)", "')")
            + " AND "
        )

    gdf = db.select_geodataframe(
        f"""
        SELECT * FROM grid.egon_etrago_bus
        WHERE {where_clause} scn_name = '{from_scn}' AND
        country = 'DE'
        """,
        epsg=4326
    )

    gdf.loc[gdf["scn_name"] == from_scn, "scn_name"] = to_scn

    db.execute_sql(
        f"""
        DELETE FROM grid.egon_etrago_bus
        WHERE {where_clause} scn_name = '{to_scn}' AND
        country = 'DE'
        """
    )

    gdf.to_postgis(
        "egon_etrago_bus",
        schema="grid",
        if_exists="append",
        con=db.engine(),
        dtype={"geom": Geometry()},
    )
