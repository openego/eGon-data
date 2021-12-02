"""The central module containing all code dealing with heat sector in etrago
"""
from egon.data import config, db
from egon.data.datasets.insert_etrago_buses import (
    finalize_bus_insertion,
    initialise_bus_insertion,
)


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
    # electrical buses related to saltcavern storage
    el_buses = db.select_dataframe(
        f"""
        SELECT bus_id
        FROM  {sources['saltcavern_data']['schema']}.
        {sources['saltcavern_data']['table']}"""
    )["bus_id"]

    # locations of electrical buses
    locations = db.select_geodataframe(
        f"""
        SELECT bus_id, geom
        FROM  {sources['buses']['schema']}.
        {sources['buses']['table']}""",
        index_col="bus_id",
    ).to_crs(epsg=4326)

    # filter by related electrical buses and drop duplicates
    locations = locations.loc[el_buses]
    locations = locations[~locations.index.duplicated(keep="first")]

    # AC bus ids and respective hydrogen bus ids are written to db for
    # later use (hydrogen storage mapping)
    AC_bus_ids = locations.index.copy()

    # create H2 bus data
    hydrogen_bus_ids = finalize_bus_insertion(locations, carrier, target)

    gdf_H2_cavern = hydrogen_bus_ids[["bus_id"]].rename(
        columns={"bus_id": "bus_H2"}
    )
    gdf_H2_cavern["bus_AC"] = AC_bus_ids
    gdf_H2_cavern["scn_name"] = hydrogen_bus_ids["scn_name"]

    # Insert data to db
    gdf_H2_cavern.to_sql(
        "egon_etrago_ac_h2",
        db.engine(),
        schema="grid",
        index=False,
        if_exists="replace",
    )


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
    # CH4 bus ids and respective hydrogen bus ids are written to db for
    # later use (CH4 grid to H2 links)
    CH4_bus_ids = gdf_H2[["bus_id", "scn_name"]].copy()

    H2_bus_ids = finalize_bus_insertion(gdf_H2, carrier, target)

    gdf_H2_CH4 = H2_bus_ids[["bus_id"]].rename(columns={"bus_id": "bus_H2"})
    gdf_H2_CH4["bus_CH4"] = CH4_bus_ids["bus_id"]
    gdf_H2_CH4["scn_name"] = CH4_bus_ids["scn_name"]

    # Insert data to db
    gdf_H2_CH4.to_sql(
        "egon_etrago_ch4_h2",
        engine,
        schema="grid",
        index=False,
        if_exists="replace",
    )
