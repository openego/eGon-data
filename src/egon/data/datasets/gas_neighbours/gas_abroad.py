"""Module containing functions to insert gas abroad

In this module, functions useful to insert the gas components (H2 and
CH4) abroad for eGon2035 and eGon100RE are defined.

"""

import zipfile

from geoalchemy2.types import Geometry
import geopandas as gpd
import pandas as pd

from egon.data import config, db


def get_foreign_gas_bus_id(carrier="CH4", scn_name="eGon2035"):
    """Calculate the etrago bus id based on the geometry

    Mapp node_ids from TYNDP and etragos bus_id

    Parameters
    ----------
    carrier : str
        Name of the carrier
    scn_name : str
        Name of the scenario

    Returns
    -------
    pandas.Series
        List of mapped node_ids from TYNDP and etragos bus_id

    """
    sources = config.datasets()["gas_neighbours"]["sources"]

    bus_id = db.select_geodataframe(
        f"""
        SELECT bus_id, ST_Buffer(geom, 1) as geom, country
        FROM grid.egon_etrago_bus
        WHERE scn_name = '{scn_name}'
        AND carrier = '{carrier}'
        AND country != 'DE'
        """,
        epsg=3035,
    )

    if scn_name is "eGon2035":
        # insert installed capacities
        file = zipfile.ZipFile(f"tyndp/{sources['tyndp_capacities']}")

        # Select buses in neighbouring countries as geodataframe
        buses = pd.read_excel(
            file.open("TYNDP-2020-Scenario-Datafile.xlsx").read(),
            sheet_name="Nodes - Dict",
        ).query("longitude==longitude")
        buses = gpd.GeoDataFrame(
            buses,
            crs=4326,
            geometry=gpd.points_from_xy(buses.longitude, buses.latitude),
        ).to_crs(3035)

        buses["bus_id"] = 0

        # Select bus_id from etrago with shortest distance to TYNDP node
        for i, row in buses.iterrows():
            distance = bus_id.set_index("bus_id").geom.distance(row.geometry)
            buses.loc[i, "bus_id"] = distance[
                distance == distance.min()
            ].index.values[0]

        return buses.set_index("node_id").bus_id


def insert_gas_grid_capacities(Neighbouring_pipe_capacities_list, scn_name):
    """Insert crossbordering gas pipelines in the database

    This function insert a list of crossbordering gas pipelines after
    cleaning the database.
    For eGon2035, all the CH4 crossbordering pipelines are inserted
    there (no H2 grid in this scenario).
    For eGon100RE, only the the crossbordering pipelines with Germany
    are inserted there (the other ones are inerted in PypsaEurSec),
    but in this scenario there are H2 and CH4 pipelines.

    Parameters
    ----------
    Neighbouring_pipe_capacities_list : pandas.DataFrame
        List of the crossbordering gas pipelines
    scn_name : str
        Name of the scenario

    Returns
    -------
    None

    """
    sources = config.datasets()["gas_neighbours"]["sources"]
    targets = config.datasets()["gas_neighbours"]["targets"]

    # Delete existing data
    if scn_name == "eGon2035":
        carrier_link = "CH4"
        carrier_bus = "CH4"

        db.execute_sql(
            f"""
            DELETE FROM 
            {sources['links']['schema']}.{sources['links']['table']}
            WHERE "bus0" IN (
                SELECT bus_id FROM 
                {sources['buses']['schema']}.{sources['buses']['table']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}'
                    AND scn_name = '{scn_name}')
            OR "bus1" IN (
                SELECT bus_id FROM 
                {sources['buses']['schema']}.{sources['buses']['table']}
                    WHERE country != 'DE'
                    AND carrier = '{carrier_bus}' 
                    AND scn_name = '{scn_name}')
            AND scn_name = '{scn_name}'
            AND carrier = '{carrier_link}'            
            ;
            """
        )

    carriers = {
        "CH4": {"bus_inDE": "CH4", "bus_abroad": "CH4"},
        "H2_retrofit": {"bus_inDE": "H2_grid", "bus_abroad": "H2"},
    }

    if scn_name == "eGon100RE":
        for c in carriers:
            db.execute_sql(
                f"""
                DELETE FROM
                {sources['links']['schema']}.{sources['links']['table']}
                WHERE ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = '{carriers[c]["bus_abroad"]}'
                        AND scn_name = '{scn_name}')
                    AND "bus1" IN (SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]["bus_inDE"]}' 
                        AND scn_name = '{scn_name}'))
                OR ("bus0" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country = 'DE'
                        AND carrier = '{carriers[c]["bus_inDE"]}'
                        AND scn_name = '{scn_name}')
                AND "bus1" IN (
                        SELECT bus_id FROM 
                        {sources['buses']['schema']}.{sources['buses']['table']}
                        WHERE country != 'DE'
                        AND carrier = '{carriers[c]["bus_abroad"]}' 
                        AND scn_name = '{scn_name}'))
                AND scn_name = '{scn_name}'
                AND carrier = '{c.index}'            
                ;
                """
            )

    # Insert data to db
    Neighbouring_pipe_capacities_list.set_geometry(
        "geom", crs=4326
    ).to_postgis(
        "egon_etrago_gas_link",
        db.engine(),
        schema="grid",
        index=False,
        if_exists="replace",
        dtype={"geom": Geometry(), "topo": Geometry()},
    )

    db.execute_sql(
        f"""
    select UpdateGeometrySRID('grid', 'egon_etrago_gas_link', 'topo', 4326) ;

    INSERT INTO {targets['links']['schema']}.{targets['links']['table']} (
        scn_name, link_id, carrier,
        bus0, bus1, p_nom, length, geom, topo)
    
    SELECT scn_name, link_id, carrier, bus0, bus1, p_nom, length, geom, topo

    FROM grid.egon_etrago_gas_link;

    DROP TABLE grid.egon_etrago_gas_link;
        """
    )
